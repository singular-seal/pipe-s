package binlog

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/metrics"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// ReplicationModeFilepos is Mysql binlog file position replication
	ReplicationModeFilepos = "filepos"
	// ReplicationModeGtid is Mysql GTID replication
	ReplicationModeGtid = "gtid"
)

const (
	NoSwitch    = 0
	SwitchByIP  = 1 // master slave switch in a list of ip
	SwitchByDNS = 2 // switch by dns change
)

const (
	FakeMysqlSlaveHostName        = "pipe_s_binlog_extractor_server"
	DefaultGoMysqlRecvBufferSize  = 1024 * 1024 * 10
	DefaultRingBufferSize         = 1024 * 8
	DefaultBinlogParseConcurrency = 4
)

var TokenBegin = []byte("BEGIN")

type MysqlBinlogInputConfig struct {
	ID              string
	Address         string // the mysql server ip:port address
	BackupAddress   string // the backup server address, if connecting Address fails will try this
	User            string
	Password        string
	ReplicationMode string // gtid or filepos
	SyncDDL         bool   // whether generate events for ddl
	SyncTxInfo      bool   // whether generate events for transaction boundaries
	SwitchByDNS     bool   // if switch mysql master slave by changing dns
}

type address struct {
	host string
	port uint16
}

func newAddressFromString(ads string) (*address, error) {
	segs := strings.Split(ads, ":")
	if len(segs) != 2 {
		return nil, errors.Errorf("wrong address:%s", ads)
	}
	addr := &address{}
	addr.host = segs[0]
	value, err := strconv.Atoi(segs[1])
	if err != nil {
		return nil, err
	}
	addr.port = uint16(value)
	return addr, err
}

// MysqlBinlogInput extracts binlog events from mysql server.
type MysqlBinlogInput struct {
	*core.BaseInput
	Config          *MysqlBinlogInputConfig
	mysqlAddress    *address // address of the mysql server providing binlog
	backupAddress   *address // backup server for current mysql server is down
	mysqlSwitchType int      // how to handle mysql fail over

	serverStatus     *ServerStatus
	stateInitialized bool
	replicationMode  string // sync by gtid or filepos

	sqlParser     *parser.Parser     // sql query string parser
	schemaStore   schema.SchemaStore // the schema store to load table schemas
	syncer        *replication.DisruptorBinlogSyncer
	eventConsumer *EventConsumer

	lastAckMsg   atomic.Value // the last acknowledged message
	lastAckError atomic.Value // the last acknowledged error received

	dnsTracker *DNSTracker // track dns change
}

func NewMysqlBinlogInput() *MysqlBinlogInput {
	eventConsumer := &EventConsumer{}
	input := &MysqlBinlogInput{
		BaseInput:     core.NewBaseInput(),
		eventConsumer: eventConsumer,
		sqlParser:     parser.New(),
	}
	eventConsumer.input = input
	eventConsumer.committed = true
	eventConsumer.currPos = &core.MysqlBinlogPosition{}
	return input
}

func (in *MysqlBinlogInput) Configure(config core.StringMap) (err error) {
	c := &MysqlBinlogInputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	// use gtid replication by default
	if len(c.ReplicationMode) == 0 {
		c.ReplicationMode = ReplicationModeGtid
	}

	in.Config = c
	in.ID = c.ID

	if in.mysqlAddress, err = newAddressFromString(c.Address); err != nil {
		return
	}
	if len(c.BackupAddress) > 0 {
		if in.backupAddress, err = newAddressFromString(c.BackupAddress); err != nil {
			return
		}
	}

	if in.Config.SwitchByDNS && net.ParseIP(in.mysqlAddress.host) == nil {
		in.mysqlSwitchType = SwitchByDNS
	} else if in.backupAddress != nil {
		in.mysqlSwitchType = SwitchByIP
	} else {
		in.mysqlSwitchType = NoSwitch
	}
	return
}

// initSyncer initialise go mysql binlog syncer
func (in *MysqlBinlogInput) initSyncer(addr *address) {
	initGoMysqlLogger(in.GetLogger())

	config := replication.DisruptorBinlogSyncerConfig{
		BinlogSyncerConfig: replication.BinlogSyncerConfig{
			ServerID:  utils.GenerateRandomServerID(),
			Flavor:    "mysql",
			Host:      addr.host,
			Port:      addr.port,
			User:      in.Config.User,
			Password:  in.Config.Password,
			Localhost: FakeMysqlSlaveHostName,

			ReadTimeout:     60 * time.Second,
			HeartbeatPeriod: 10 * time.Second,
			RecvBufferSize:  DefaultGoMysqlRecvBufferSize,
			UseDecimal:      true,
		},
		BufferSize:            DefaultRingBufferSize,
		ParseConcurrency:      DefaultBinlogParseConcurrency,
		SkipGTIDSetManagement: true,
		WriteBlockParkTime:    100 * time.Nanosecond,
	}

	in.syncer = replication.NewDisruptorBinlogSyncer(config, in.eventConsumer)
	in.GetLogger().Info("init binlog syncer", log.Uint32("ServerID", config.ServerID))
}

func (in *MysqlBinlogInput) startFromAddress(addr *address) (err error) {
	in.GetLogger().Info("start from address", log.String("host", addr.host), log.Uint16("port", addr.port))
	if in.schemaStore, err = schema.NewSimpleSchemaStoreWithParameters(addr.host, addr.port,
		in.Config.User, in.Config.Password); err != nil {
		return
	}
	in.schemaStore.SetLogger(in.GetLogger())

	in.serverStatus, err = LoadFromServer(addr.host, addr.port, in.Config.User, in.Config.Password)
	if err != nil {
		in.GetLogger().Error("failed load mysql server status", log.Error(err))
		return
	}
	in.GetLogger().Info("mysql server status loaded", log.String("status", in.serverStatus.String()))

	// init and start sync
	in.initSyncer(addr)
	if err = in.startSync(); err != nil {
		in.GetLogger().Error("failed to start syncing", log.Error(err))
		return
	}
	return
}

func (in *MysqlBinlogInput) Start() (err error) {
	// in SwitchByIP mode try mysqlAddress first if fails try backupAddress
	if in.mysqlSwitchType == SwitchByIP {
		if err = in.startFromAddress(in.mysqlAddress); err == nil {
			return
		}
		in.GetLogger().Info("failed to start from first mysql", log.Error(err))
		in.clearResource()
		if err = in.startFromAddress(in.backupAddress); err == nil {
			return
		}
		in.GetLogger().Error("failed to start from backup mysql", log.Error(err))
		return
	}

	if err = in.startFromAddress(in.mysqlAddress); err != nil {
		return
	}
	if in.mysqlSwitchType == SwitchByDNS {
		in.dnsTracker = NewDNSTracker(in.mysqlAddress.host, in.GetLogger(), func() {
			in.RaiseError(errors.Errorf("dns change detected"))
		})
		in.dnsTracker.Start()
	}
	return
}

func (in *MysqlBinlogInput) resolveInitState() (err error) {
	// set default init state to latest pos in DB if state is not set
	if !in.stateInitialized {
		pos, err := in.serverStatus.BinlogPosition()
		if err != nil {
			return err
		}
		in.eventConsumer.currPos = &pos
		in.stateInitialized = true
	}

	// todo find position by timestamp
	// merge purged gtidset into full gtidset to solve the issue that start syncing with gtidset which doesn't
	// include the purged gtidset and fails with error.
	if err = mergeGTIDSets(in.serverStatus.PurgedGtidSet, in.eventConsumer.currPos.FullGTIDSet); err != nil {
		return
	}
	return
}

func mergeGTIDSets(from string, to mysql.GTIDSet) error {
	segs := strings.Split(from, ",")
	for _, seg := range segs {
		if len(seg) == 0 {
			continue
		}
		if err := to.Update(seg); err != nil {
			return err
		}
	}
	return nil
}

func (in *MysqlBinlogInput) startSync() (err error) {
	if err = in.resolveInitState(); err != nil {
		return
	}

	if err = in.setReplicationMode(); err != nil {
		return
	}

	switch in.replicationMode {
	case ReplicationModeFilepos:
		pos := mysql.Position{
			Name: in.eventConsumer.currPos.BinlogName,
			Pos:  in.eventConsumer.currPos.TxBinlogPos,
		}
		err = in.syncer.StartSync(pos)
	case ReplicationModeGtid:
		err = in.syncer.StartSyncGTID(in.eventConsumer.currPos.FullGTIDSet)
	default:
		err = errors.Errorf("unknown replication mode:%s", in.replicationMode)
	}
	return
}

func (in *MysqlBinlogInput) setReplicationMode() error {
	switch in.Config.ReplicationMode {
	case ReplicationModeGtid:
		if !in.serverStatus.GtidEnabled {
			return errors.Errorf("gtid not supported by mysql server")
		}
		in.replicationMode = ReplicationModeGtid
	case ReplicationModeFilepos:
		in.replicationMode = ReplicationModeFilepos

	default:
		if in.serverStatus.GtidEnabled {
			in.replicationMode = ReplicationModeGtid
		} else {
			in.replicationMode = ReplicationModeFilepos
		}
	}
	return nil
}

// genEventID generates unique id for each row change event.
func (in *MysqlBinlogInput) genEventID(pos *core.MysqlBinlogPosition, transactionOffset int, rowOffset int) string {
	switch in.replicationMode {
	case ReplicationModeGtid:
		return fmt.Sprintf("%s.%d.%d", pos.ServerUUID, pos.TransactionID, transactionOffset)
	default:
		return fmt.Sprintf("%d.%s.%d.%d", pos.ServerID, pos.BinlogName, pos.BinlogPos, rowOffset)
	}
}

// EventConsumer consumes binlog event from DisruptorBinlogSyncer.
type EventConsumer struct {
	currPos           *core.MysqlBinlogPosition
	input             *MysqlBinlogInput
	transactionOffset int  // the offset in current transaction
	committed         bool // whether current transaction is committed
}

func (c *EventConsumer) Handle(event *replication.BinlogEvent) (err error) {
	c.currPos.ServerID = event.Header.ServerID
	if event.Header.Timestamp > 0 {
		c.currPos.Timestamp = event.Header.Timestamp
		metrics.UpdateTaskBinlogDelay(event.Header.Timestamp)
	}

	err = nil
	c.currPos.BinlogPos = event.Header.LogPos
	switch e := event.Event.(type) {
	case *replication.RotateEvent:
		c.currPos.BinlogName = string(e.NextLogName)
		c.currPos.BinlogPos = uint32(e.Position)
	case *replication.GTIDEvent:
		err = c.handleGTIDEvent(e)

	case *replication.XIDEvent:
		err = c.handleTxCommit()

	case *replication.RowsEvent:
		err = c.handleRowsEvent(c.currPos, event)

	case *replication.QueryEvent:
		err = c.handleQueryEvent(c.currPos, e)

	case *replication.BeginLoadQueryEvent:
	case *replication.ExecuteLoadQueryEvent:
	case *replication.GenericEvent:
	case *replication.FormatDescriptionEvent:
	case *replication.TableMapEvent:
	case *replication.PreviousGTIDsEvent:

	default:
		c.input.GetLogger().Info("skipped binlog event type", log.String("type", utils.GetTypeName(e)),
			log.String("id", c.input.genEventID(c.currPos, 0, 0)))
	}

	if err != nil {
		c.input.GetLogger().Error("failed to handle event", log.String("type",
			utils.GetTypeName(utils.GetTypeName(event.Event))), log.String("id", c.input.genEventID(c.currPos,
			0, 0)), log.Error(err))
		c.input.RaiseError(err)
	}
	return
}

func (c *EventConsumer) HandleError(err error) {
	c.input.GetLogger().Error("HandleError", log.String("comp", "EventConsumer"), log.Error(err))
	//todo maybe need to handle reconnection logic here later
	c.input.RaiseError(err)
}

func (c *EventConsumer) Close() {
	c.input.GetLogger().Info("Close invoked", log.String("comp", "EventConsumer"))
}

// handleGTIDEvent handles gtid event and update current replication position.
func (c *EventConsumer) handleGTIDEvent(e *replication.GTIDEvent) (err error) {
	// ensure last transaction been committed before starting a new one
	if !c.committed {
		if err = c.handleTxCommit(); err != nil {
			return
		}
	}
	// update GTID in current position
	serverUUID, err := uuid.FromBytes(e.SID)
	if err != nil {
		c.input.GetLogger().Error("failed parsing uuid", log.Error(err))
		return
	}
	c.committed = false
	c.currPos.ServerUUID = serverUUID.String()
	c.currPos.TransactionID = e.GNO
	c.transactionOffset = -1
	return
}

// handleTxCommit advances current gtidset by merge current gtid into it.
func (c *EventConsumer) handleTxCommit() (err error) {
	if c.committed {
		return
	}

	if c.input.replicationMode == ReplicationModeGtid {
		gtid := fmt.Sprintf("%s:%d", c.currPos.ServerUUID, c.currPos.TransactionID)
		//todo optimize update function
		if err = c.currPos.FullGTIDSet.Update(gtid); err != nil {
			c.input.GetLogger().Error("failed to update gtid", log.String("gtid", gtid), log.Error(err))
			return
		}
		c.currPos.FullGTIDSetString = c.currPos.FullGTIDSet.String()
	} else {
		c.currPos.TxBinlogPos = c.currPos.BinlogPos
	}
	c.committed = true
	return
}

// handleRowsEvent handles mysql row change events.
func (c *EventConsumer) handleRowsEvent(pos *core.MysqlBinlogPosition, e *replication.BinlogEvent) (err error) {
	rowsEvent := e.Event.(*replication.RowsEvent)
	createTime := uint64(time.Now().UnixNano())
	dbName := string(rowsEvent.Table.Schema)
	tableName := string(rowsEvent.Table.Table)
	fullTableName := dbName + "." + tableName

	var op string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0:
		fallthrough
	case replication.WRITE_ROWS_EVENTv1:
		fallthrough
	case replication.WRITE_ROWS_EVENTv2:
		op = core.DBInsert
	case replication.UPDATE_ROWS_EVENTv0:
		fallthrough
	case replication.UPDATE_ROWS_EVENTv1:
		fallthrough
	case replication.UPDATE_ROWS_EVENTv2:
		op = core.DBUpdate
	case replication.DELETE_ROWS_EVENTv0:
		fallthrough
	case replication.DELETE_ROWS_EVENTv1:
		fallthrough
	case replication.DELETE_ROWS_EVENTv2:
		op = core.DBDelete
	default:
		err = errors.Errorf("unknown event type:%s", e.Header.EventType.String())
		return
	}

	tableSchema, err := c.input.schemaStore.GetTable(dbName, tableName)
	if err != nil {
		return err
	}

	switch op {
	case core.DBInsert:
		for i := range rowsEvent.Rows {
			c.transactionOffset++
			m := c.newDMLMessage(pos, i, e, createTime, fullTableName, op, tableSchema)
			m.Data.(*core.MysqlDMLEvent).NewRow = rowsEvent.Rows[i]
			c.input.GetOutput().Process(m)
		}
	case core.DBUpdate:
		if len(rowsEvent.Rows)%2 != 0 {
			c.input.GetLogger().Error("odd update event rows", log.String("event_id",
				c.input.genEventID(c.currPos, c.transactionOffset, 0)),
				log.String("event", hex.EncodeToString(e.RawData)))
		}

		for i := 0; i < len(rowsEvent.Rows)-1; i += 2 {
			c.transactionOffset++
			m := c.newDMLMessage(pos, i/2, e, createTime, fullTableName, op, tableSchema)
			m.Data.(*core.MysqlDMLEvent).OldRow = rowsEvent.Rows[i]
			m.Data.(*core.MysqlDMLEvent).NewRow = rowsEvent.Rows[i+1]
			c.input.GetOutput().Process(m)
		}
	case core.DBDelete:
		for i := range rowsEvent.Rows {
			c.transactionOffset++
			m := c.newDMLMessage(pos, i, e, createTime, fullTableName, op, tableSchema)
			m.Data.(*core.MysqlDMLEvent).OldRow = rowsEvent.Rows[i]
			c.input.GetOutput().Process(m)
		}
	}
	return
}

func (c *EventConsumer) handleQueryEvent(pos *core.MysqlBinlogPosition, e *replication.QueryEvent) error {
	// BEGIN comes after every GTID event can be ignored.
	if bytes.Equal(e.Query, TokenBegin) {
		return nil
	}

	// update schema store if needed
	stmt, err := c.input.sqlParser.ParseOneStmt(string(e.Query), "", "")
	if err != nil {
		c.input.GetLogger().Warn("fail parse query statement", log.Error(err), log.String("sql", string(e.Query)))
		// maybe unsupported sql, can ignore it safely
		return c.handleTxCommit()
	}
	infos := utils.ExtractFromDDL(e.Schema, stmt)
	for _, each := range infos {
		if len(each.DB) > 0 {
			if err := c.input.schemaStore.DeleteTable(each.DB, each.Table); err != nil {
				return err
			}
		}
	}

	// https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	// handle implicit commit
	return c.handleTxCommit()
}

func (c *EventConsumer) newDMLMessage(pos *core.MysqlBinlogPosition, rowIndex int, e *replication.BinlogEvent,
	createTime uint64, table string, op string, ts *core.Table) *core.Message {

	pos.TransactionOffset = c.transactionOffset
	pos.RowOffset = rowIndex

	m := core.NewMessage(core.TypeDML)
	m.Header.ID = c.input.genEventID(pos, pos.TransactionOffset, pos.RowOffset)
	m.Header.CreateTime = createTime

	mysqlEvent := &core.MysqlDMLEvent{
		Pos:           pos.SimpleCopy(),
		BinlogEvent:   e,
		FullTableName: table,
		Operation:     op,
	}

	m.SetMeta(core.MetaMySqlPos, mysqlEvent.Pos)
	m.SetMeta(core.MetaTableSchema, ts)
	m.Data = mysqlEvent
	return m
}

func (in *MysqlBinlogInput) Ack(msg *core.Message, err error) {
	if err != nil {
		in.lastAckError.Store(err)
	}
	if msg != nil {
		in.lastAckMsg.Store(msg)
	}
}

func (in *MysqlBinlogInput) SetState(state []byte) (err error) {
	if state == nil || len(state) == 0 {
		return
	}
	if core.UnmarshalMysqlBinlogPosition(in.eventConsumer.currPos, state); err != nil {
		return
	}
	in.stateInitialized = true
	return
}

func (in *MysqlBinlogInput) getLastAck() (msg *core.Message, err error) {
	if val := in.lastAckMsg.Load(); val != nil {
		msg = val.(*core.Message)
	}
	if val := in.lastAckError.Load(); val != nil {
		err = val.(error)
	}
	return
}

func (in *MysqlBinlogInput) GetState() ([]byte, bool) {
	m, lastErr := in.getLastAck()
	if m == nil {
		return nil, false
	}
	obj, ok := m.GetMeta(core.MetaMySqlPos)
	if !ok || obj == nil {
		in.GetLogger().Error("position not found in meta", log.String("message_id", m.Header.ID))
		return nil, true
	}
	pos := obj.(*core.MysqlBinlogPosition)
	state, err := core.MarshalMysqlBinlogPosition(pos)
	if err != nil {
		in.GetLogger().Error("pos marshal error", log.Any("pos", pos), log.Error(err))
		return nil, true
	}
	if lastErr != nil {
		in.GetLogger().Error("ack error received", log.Any("pos", pos), log.Error(lastErr))
		return nil, true
	} else {
		return state, false
	}
}

func (in *MysqlBinlogInput) clearResource() {
	if in.syncer != nil {
		in.syncer.Close()
	}
	if !utils.IsNil(in.schemaStore) {
		in.schemaStore.Close()
	}
}

func (in *MysqlBinlogInput) Stop() {
	if in.mysqlSwitchType == SwitchByDNS {
		in.dnsTracker.Stop()
	}
	in.clearResource()
}

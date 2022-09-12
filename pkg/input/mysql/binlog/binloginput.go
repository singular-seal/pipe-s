package binlog

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
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
	// MysqlFailOverByVIP will not change ip after failing over
	MysqlFailOverByVIP = 0
	// MysqlFailOverByIP will change ip after failing over
	MysqlFailOverByIP = 1
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
	Address         string   // the mysql server ip:port address
	BackupAddresses []string // the backup servers addresses, if connecting Address fails will try these
	User            string
	Password        string
	ReplicationMode string // gtid or filepos
	SyncDDL         bool   // whether generate events for ddl
	SyncTxInfo      bool   // whether generate events for transaction boundaries
}

type address struct {
	host string
	port uint16
}

func newAddressFromString(ads string) (*address, error) {
	segs := strings.Split(ads, ":")
	if len(segs) != 2 {
		return nil, fmt.Errorf("wrong address:%s", ads)
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
	Config            *MysqlBinlogInputConfig
	mysqlAddress      *address   // address of the mysql server providing binlog
	backupAddresses   []*address // backup servers for current mysql server is down
	mysqlFailOverType int        // how to handle mysql fail over

	serverStatus     *ServerStatus
	stateInitialized bool
	replicationMode  string // sync by gtid or filepos

	syncer        *replication.DisruptorBinlogSyncer
	eventConsumer *EventConsumer

	lastAckMsg   atomic.Value // the last acknowledged message
	lastAckError atomic.Value // the last acknowledged error received
}

func NewMysqlBinlogInput() *MysqlBinlogInput {
	eventConsumer := &EventConsumer{}
	input := &MysqlBinlogInput{
		BaseInput:       core.NewBaseInput(),
		backupAddresses: make([]*address, 0),
		eventConsumer:   eventConsumer,
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
	for _, each := range c.BackupAddresses {
		if addr, err := newAddressFromString(each); err == nil {
			in.backupAddresses = append(in.backupAddresses, addr)
		} else {
			return err
		}
	}
	if len(in.backupAddresses) > 0 {
		in.mysqlFailOverType = MysqlFailOverByIP
	} else {
		in.mysqlFailOverType = MysqlFailOverByVIP
	}
	return
}

// initSyncer initialise go mysql binlog syncer
func (in *MysqlBinlogInput) initSyncer() {
	initGoMysqlLogger(in.GetLogger())

	config := replication.DisruptorBinlogSyncerConfig{
		BinlogSyncerConfig: replication.BinlogSyncerConfig{
			ServerID:  utils.GenerateRandomServerID(),
			Flavor:    "mysql",
			Host:      in.mysqlAddress.host,
			Port:      in.mysqlAddress.port,
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
	}

	in.syncer = replication.NewDisruptorBinlogSyncer(config, in.eventConsumer)
	in.GetLogger().Info("init binlog syncer", log.Uint32("ServerID", config.ServerID))
}

func (in *MysqlBinlogInput) Start() (err error) {
	in.serverStatus, err = LoadFromServer(in.mysqlAddress.host, in.mysqlAddress.port, in.Config.User, in.Config.Password)
	if err != nil {
		in.GetLogger().Error("failed load mysql server status", log.Error(err))
		return
	}
	in.GetLogger().Info("mysql server status loaded", log.String("status", in.serverStatus.String()))

	// init and start sync
	in.initSyncer()
	if err = in.startSync(); err != nil {
		in.GetLogger().Error("failed to start syncing", log.Error(err))
		return
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
			Pos:  in.eventConsumer.currPos.BinlogPos,
		}
		err = in.syncer.StartSync(pos)
	case ReplicationModeGtid:
		err = in.syncer.StartSyncGTID(in.eventConsumer.currPos.FullGTIDSet)
	default:
		err = fmt.Errorf("unknown replication mode:%s", in.replicationMode)
	}
	if err != nil {
		return
	}

	/*	err = s.startTrackHostnameChanges()
		if err != nil {
			s.Logger.WithError(err).Error("start_hostname_change_track_eror")
			return
		}
	*/

	return
}

func (in *MysqlBinlogInput) setReplicationMode() error {
	switch in.Config.ReplicationMode {
	case ReplicationModeGtid:
		if !in.serverStatus.GtidEnabled {
			return fmt.Errorf("gtid not supported by mysql server")
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
		return nil
	}

	gtid := fmt.Sprintf("%s:%d", c.currPos.ServerUUID, c.currPos.TransactionID)
	//todo optimize update function
	if err = c.currPos.FullGTIDSet.Update(gtid); err != nil {
		c.input.GetLogger().Error("failed to update gtid", log.String("gtid", gtid), log.Error(err))
		return
	}
	c.currPos.FullGTIDSetString = c.currPos.FullGTIDSet.String()
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
		err = fmt.Errorf("unknown event type:%s", e.Header.EventType.String())
		return
	}

	switch op {
	case core.DBInsert:
		for i := range rowsEvent.Rows {
			c.transactionOffset++
			m := c.newDMLMessage(pos, i, e, createTime, fullTableName, op)
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
			m := c.newDMLMessage(pos, i/2, e, createTime, fullTableName, op)
			m.Data.(*core.MysqlDMLEvent).OldRow = rowsEvent.Rows[i]
			m.Data.(*core.MysqlDMLEvent).NewRow = rowsEvent.Rows[i+1]
			c.input.GetOutput().Process(m)
		}
	case core.DBDelete:
		for i := range rowsEvent.Rows {
			c.transactionOffset++
			m := c.newDMLMessage(pos, i, e, createTime, fullTableName, op)
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
	// https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	// handle implicit commit
	return c.handleTxCommit()
}

func (c *EventConsumer) newDMLMessage(pos *core.MysqlBinlogPosition, rowIndex int, e *replication.BinlogEvent,
	createTime uint64, table string, op string) *core.Message {

	pos.TransactionOffset = c.transactionOffset
	pos.RowOffset = rowIndex

	m := core.NewMessage(core.TypeDML)
	m.Header.ID = c.input.genEventID(pos, pos.TransactionOffset, pos.RowOffset)
	m.Header.CreateTime = createTime

	mysqlEvent := &core.MysqlDMLEvent{
		Pos:      pos.SimpleCopy(),
		RawEvent: e,
		Table:    table,
		OPType:   op,
	}

	m.SetMeta(core.MetaMySQLPos, mysqlEvent.Pos)
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
	obj, ok := m.GetMeta(core.MetaMySQLPos)
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
		in.GetLogger().Error("ack error received", log.Any("pos", pos), log.Error(err))
		return nil, true
	} else {
		return state, false
	}
}

func (in *MysqlBinlogInput) Stop() {
	in.syncer.Close()
}

package batch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
)

const (
	DefaultMaxConnections     = 100
	DefaultTableConcurrency   = 1
	DefaultFlushIntervalMS    = 100
	DefaultFlushBatchSize     = 3000
	DefaultSqlBatchSize       = 1000
	DefaultMaxComboKeyColumns = 5
)

// MysqlBatchOutput has some restrictions 1.target table must have pk 2.column counts in pk should be less than 5
type MysqlBatchOutput struct {
	*core.BaseOutput
	config          *MysqlBatchOutputConfig
	tableProcessors map[string][]*TableProcessor
	conn            *sql.DB
	schemaStore     schema.SchemaStore // the schema store to load table schemas

	stopCtx    context.Context
	stopCancel context.CancelFunc
}

type MysqlBatchOutputConfig struct {
	ID               string
	Host             string // standard mysql host, port, user and password config
	Port             uint16
	User             string
	Password         string
	MaxConnections   int   // max connection count
	TableConcurrency int   // processor count for each table
	FlushIntervalMS  int64 // batch flush interval
	FlushBatchSize   int   // batch flush size
	// max sql statements sent in one api call, restricted by column counts and mysql server side restriction
	// needn't change this normally
	SqlBatchSize int
	// if exec insert, update, delete and replace micro batches concurrently, will cost more db connections
	ExecCRUDConcurrentlyInBatch bool
}

func NewMysqlBatchOutput() *MysqlBatchOutput {
	ctx, cancelFunc := context.WithCancel(context.Background())

	output := &MysqlBatchOutput{
		BaseOutput:      core.NewBaseOutput(),
		tableProcessors: make(map[string][]*TableProcessor),
		stopCtx:         ctx,
		stopCancel:      cancelFunc,
	}
	return output
}

func (o *MysqlBatchOutput) Start() (err error) {
	if o.conn, err = utils.CreateMysqlConnection(o.config.Host, o.config.Port, o.config.User, o.config.Password); err != nil {
		return
	}
	o.conn.SetMaxIdleConns(o.config.MaxConnections)
	o.schemaStore = schema.NewSimpleSchemaStoreWithClient(o.conn)
	o.schemaStore.SetLogger(o.GetLogger())
	return nil
}

func (o *MysqlBatchOutput) Stop() {
	o.stopCancel()
	o.schemaStore.Close()
	if err := utils.CloseMysqlConnection(o.conn); err != nil {
		o.GetLogger().Error("MysqlBatchOutput failed close db connection")
	}
}

func (o *MysqlBatchOutput) Configure(config core.StringMap) (err error) {
	c := &MysqlBatchOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c

	if o.config.MaxConnections == 0 {
		o.config.MaxConnections = DefaultMaxConnections
	}
	if o.config.TableConcurrency == 0 {
		o.config.TableConcurrency = DefaultTableConcurrency
	}

	if o.config.FlushIntervalMS == 0 {
		o.config.FlushIntervalMS = DefaultFlushIntervalMS
	}

	if o.config.FlushBatchSize == 0 {
		o.config.FlushBatchSize = DefaultFlushBatchSize
	}

	if o.config.SqlBatchSize == 0 {
		o.config.SqlBatchSize = DefaultSqlBatchSize
	}
	return nil
}

func (o *MysqlBatchOutput) Process(m *core.Message) {
	dbChange := m.Data.(*core.DBChangeEvent)
	ts, err := o.schemaStore.GetTable(dbChange.Database, dbChange.Table)
	if err != nil {
		o.GetInput().Ack(m, err)
		return
	}
	// TODO: can support more generic unique key in future
	pk := getPKValue(dbChange, ts)
	if len(pk) > DefaultMaxComboKeyColumns {
		o.GetInput().Ack(m, errors.Errorf("don't support pk columns more than %d", DefaultMaxComboKeyColumns))
		return
	}
	// get processors for current table
	processors := o.getTableProcessors(dbChange)
	// index number in table processors
	index := o.getProcessorIndex(pk)
	info := &MessageInfo{
		key:      copyKey(pk),
		dbChange: dbChange,
		message:  m,
	}
	processors[index].Process(info)
}

func getPKValue(event *core.DBChangeEvent, ts *core.Table) []interface{} {
	result := make([]interface{}, len(ts.PKColumns))
	for i := 0; i < len(ts.PKColumns); i++ {
		if event.Operation == core.DBDelete {
			result[i] = event.OldRow[ts.PKColumns[i].Name]
		} else {
			result[i] = event.NewRow[ts.PKColumns[i].Name]
		}
	}
	return result
}

func (o *MysqlBatchOutput) getTableProcessors(event *core.DBChangeEvent) []*TableProcessor {
	ftn := utils.FullTableName(event.Database, event.Table)
	processors, ok := o.tableProcessors[ftn]
	if ok {
		return processors
	}
	// create processors
	processors = make([]*TableProcessor, 0)
	for i := 0; i < o.config.TableConcurrency; i++ {
		processor := NewTableProcessor(o, i)
		processors = append(processors, processor)
		go processor.Run()
	}
	o.tableProcessors[ftn] = processors
	return processors
}

func (o *MysqlBatchOutput) getProcessorIndex(key []interface{}) int {
	if o.config.TableConcurrency < 2 {
		return 0
	}

	hash := calcHash(key)
	index := hash % o.config.TableConcurrency
	if index < 0 {
		index += o.config.TableConcurrency
	}

	return index
}

func calcHash(pk []interface{}) int {
	if len(pk) == 0 {
		return 0
	}
	if len(pk) == 1 {
		return utils.GetFNV64aHash(fmt.Sprintf("%v", pk))
	}
	var sb strings.Builder
	for _, each := range pk {
		sb.WriteString(fmt.Sprintf("%v", each))
	}
	return utils.GetFNV64aHash(sb.String())
}

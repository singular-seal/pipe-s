package batch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"hash/fnv"
	"strings"
)

const (
	DefaultMaxIdleConnections = 1000
	DefaultTableConcurrency   = 1
	DefaultFlushIntervalMS    = 100
	DefaultFlushBatchSize     = 3000
	DefaultSqlBatchSize       = 1000
	DefaultMaxComboKeyColumns = 5
)

type MysqlBatchOutput struct {
	*core.BaseOutput
	config          *MysqlBatchOutputConfig
	tableProcessors map[string][]*TableProcessor
	conn            *sql.DB

	stopWaitContext context.Context
	stopCancel      context.CancelFunc
}

type MysqlBatchOutputConfig struct {
	ID             string
	Host           string
	Port           uint16
	User           string
	Password       string
	MaxConnections int

	TableConcurrency int // processor count for each table
	FlushIntervalMS  int64
	FlushBatchSize   int

	InsertOnly bool // for database copy scenario
	// if exec insert, update, delete and replace micro batches concurrently, will cost more db connections
	ExecCRUDConcurrentlyInBatch bool
	// max sql statements sent in one api call, restricted by column counts and mysql server side restriction
	// needn't change this normally
	SqlBatchSize int
}

func NewMysqlBatchOutput() *MysqlBatchOutput {
	ctx, cancelFunc := context.WithCancel(context.Background())

	s := &MysqlBatchOutput{
		BaseOutput:      core.NewBaseOutput(),
		tableProcessors: make(map[string][]*TableProcessor),
		stopWaitContext: ctx,
		stopCancel:      cancelFunc,
	}
	return s
}

func (o *MysqlBatchOutput) Start() (err error) {
	if o.conn, err = utils.CreateMysqlClient(o.config.Host, o.config.Port, o.config.User, o.config.Password); err != nil {
		return
	}
	o.conn.SetMaxIdleConns(o.config.MaxConnections)
	return nil
}

func (o *MysqlBatchOutput) Stop() {
	o.stopCancel()
	if err := utils.CloseMysqlClient(o.conn); err != nil {
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
		o.config.MaxConnections = DefaultMaxIdleConnections
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
	ts, ok := m.GetTableSchema()
	if !ok {
		o.GetInput().Ack(m, core.NoSchemaError(dbChange.Database, dbChange.Table))
		return
	}
	// TODO: can support more generic unique key in future
	pk := getPKValue(dbChange, ts)
	if !o.config.InsertOnly && len(pk) > DefaultMaxComboKeyColumns {
		o.GetInput().Ack(m, fmt.Errorf("don't support pk columns more than %d", DefaultMaxComboKeyColumns))
		return
	}
	// get processors for current table
	processors := o.getTableProcessors(dbChange)
	// index number in table processors
	index := o.getProcessorIndex(pk)
	processors[index].Process(pk, dbChange, m)
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
	ftn := event.Database + "." + event.Table
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
		return getFNV64aHash(fmt.Sprintf("%v", pk))
	}
	var sb strings.Builder
	for _, each := range pk {
		sb.WriteString(fmt.Sprintf("%v", each))
	}
	return getFNV64aHash(sb.String())
}

func getFNV64aHash(text string) int {
	algorithm := fnv.New64a()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum64())
}

package batch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"hash/fnv"
)

const (
	// DefaultMaxIdleConnections is the maximum number of idle connections to the database
	DefaultMaxIdleConnections = 1000
	// DefaultConcurrency is the processor concurrency
	DefaultConcurrency = 1
	// DefaultSendBatchSize is the number of db records sent to the database each time
	DefaultSendBatchSize = 500
	// DefaultFlushIntervalMS is the flush interval for the outstream
	DefaultFlushIntervalMS = 1000
	// DefaultFlushBatchSize is the flush size for the outstream
	DefaultFlushBatchSize = 30000
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

	KeyColumns       []string // the column/columns by which we split row data to multiple processors, use pk by default
	TableConcurrency int      // processor count for each table
	FlushIntervalMS  int64
	FlushBatchSize   int
	SendBatchSize    int
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
		o.config.TableConcurrency = DefaultConcurrency
	}

	if o.config.FlushIntervalMS == 0 {
		o.config.FlushIntervalMS = DefaultFlushIntervalMS
	}

	if o.config.FlushBatchSize == 0 {
		o.config.FlushBatchSize = DefaultFlushBatchSize
	}

	if o.config.SendBatchSize == 0 {
		o.config.SendBatchSize = DefaultSendBatchSize
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

	if len(schema.PrimaryKey) != 1 {
		o.UpStream.AckMessage(m.Meta, fmt.Errorf("unsupported_pk_count_:%d", len(schema.PrimaryKey)))
		return
	}

	// get pk
	var pk interface{}
	if dbChange.Command == core.MySQLCommandDelete {
		pk = dbChange.OldRow[schema.PrimaryKey[0].Name]
	} else {
		pk = dbChange.NewRow[schema.PrimaryKey[0].Name]
	}

	processors := o.findTableProcessors(dbChange)
	partition := o.getConcurrencyPartition(pk)
	processors[partition].AddToBatch(pk, dbChange, m.Meta)
}

func (o *MysqlBatchOutput) findTableProcessors(event *core.DBChangeEvent) []*TableProcessor {
	ftn := fmt.Sprintf("%o.%o", event.Database, event.Table)
	processors, ok := o.tableProcessors[ftn]
	if ok {
		return processors
	}

	processors = make([]*TableProcessor, 0)
	// create processors
	for i := 0; i < o.config.TableConcurrency; i++ {
		proc := NewLongMysqlOutStreamProcessor(o, i)
		processors = append(processors, proc)
		go proc.Run()
	}
	o.tableProcessors[ftn] = processors
	return processors
}

func (o *MysqlBatchOutput) getConcurrencyPartition(pk interface{}) int {
	if o.config.TableConcurrency == 1 {
		return 0
	}

	hash := getFNV64aHash(fmt.Sprintf("%v", pk))
	partition := hash % o.config.TableConcurrency
	if partition < 0 {
		partition += o.config.TableConcurrency
	}

	return partition
}

func getFNV64aHash(text string) int {
	algorithm := fnv.New64a()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum64())
}

package stream

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"sync"
	"time"
)

const (
	DefaultConcurrency     = 4
	DefaultTaskQueueLength = 1000

	RoutingByTable      = "Table"
	RoutingByPrimaryKey = "PrimaryKey"
)

type MysqlStreamOutput struct {
	*core.BaseOutput
	config      *MysqlStreamOutputConfig
	schemaStore schema.SchemaStore // schema store of the target db
	conn        *sql.DB

	taskQueues      []chan *task // messages will be dispatched into different task queues for concurrent processing
	stopWaitContext context.Context
	stopCancel      context.CancelFunc
	stopWait        *sync.WaitGroup
}

type MysqlStreamOutputConfig struct {
	ID              string
	Host            string
	Port            uint16
	User            string
	Password        string
	Concurrency     int
	RoutingBy       string
	TaskQueueLength int
}

type task struct {
	message           *core.Message
	tableSchema       *core.Table
	primaryKeyColumns []string
}

func NewMysqlStreamOutput() *MysqlStreamOutput {
	output := &MysqlStreamOutput{
		BaseOutput: core.NewBaseOutput(),
	}
	return output
}

func (o *MysqlStreamOutput) Start() (err error) {
	if o.conn, err = utils.CreateMysqlClient(o.config.Host, o.config.Port, o.config.User, o.config.Password); err != nil {
		return
	}
	o.conn.SetMaxIdleConns(o.config.Concurrency)
	o.schemaStore = schema.NewSimpleSchemaStoreWithClient(o.conn)
	o.schemaStore.SetLogger(o.GetLogger())

	o.stopWaitContext, o.stopCancel = context.WithCancel(context.Background())
	o.stopWait = &sync.WaitGroup{}

	o.taskQueues = make([]chan *task, o.config.Concurrency)
	for i := 0; i < o.config.Concurrency; i++ {
		o.taskQueues[i] = make(chan *task, o.config.TaskQueueLength)
		o.stopWait.Add(1)
		go o.processTaskQueue(i)
	}

	go o.checkProgress()
	return nil
}

func (o *MysqlStreamOutput) Stop() {
	o.stopCancel()
	o.stopWait.Wait()

	o.schemaStore.Close()
	if err := utils.CloseMysqlClient(o.conn); err != nil {
		o.GetLogger().Error("MysqlBatchOutput failed close db connection")
	}
}

func (o *MysqlStreamOutput) Configure(config core.StringMap) (err error) {
	c := &MysqlStreamOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c
	if o.config.Concurrency == 0 {
		o.config.Concurrency = DefaultConcurrency
	}
	if o.config.TaskQueueLength == 0 {
		o.config.TaskQueueLength = DefaultTaskQueueLength
	}
	if len(o.config.RoutingBy) == 0 {
		o.config.RoutingBy = RoutingByTable
	}
	return nil
}

func (o *MysqlStreamOutput) processTaskQueue(index int) {
	for {
		select {
		case <-o.stopWaitContext.Done():
			o.stopWait.Done()
			return
		case t := <-o.taskQueues[index]:
			o.processTask(t)
		}
	}
}

func (o *MysqlStreamOutput) processTask(t *task) {
	event := t.message.Body.(*core.DBChangeEvent)
	sqlString, sqlArgs := utils.GenerateSqlAndArgs(event, t.primaryKeyColumns)
	result, err := o.conn.Exec(sqlString, sqlArgs...)
	if err != nil {
		o.GetInput().Ack(t.message, err)
	} else {
		if c, err1 := result.RowsAffected(); err1 != nil || c == 0 {
			o.GetLogger().Warn("no row affected", log.String("sql", sqlString),
				log.String("id", t.message.Header.ID), log.Error(err1))
		}
		o.GetInput().Ack(t.message, nil)
	}
}

func (o *MysqlStreamOutput) Process(m *core.Message) {
	dbChange := m.Body.(*core.DBChangeEvent)
	ts, err := o.schemaStore.GetTable(dbChange.Database, dbChange.Table)
	if err != nil {
		o.GetInput().Ack(m, err)
		return
	}

	m.SetMeta(core.MetaTableSchema, ts)
	pkCols := ts.PKColumnNames()
	if len(pkCols) == 0 {
		o.GetInput().Ack(m, core.NoPKError(dbChange.Database, dbChange.Table))
		return
	}

	var rk int
	if o.config.RoutingBy == RoutingByPrimaryKey {
		rk = genKeyHash(dbChange.Database, dbChange.Table, getKeyValues(dbChange, pkCols))
	} else {
		rk = genTableHash(dbChange.Database, dbChange.Table)
	}
	o.taskQueues[rk%o.config.Concurrency] <- &task{
		message:           m,
		tableSchema:       ts,
		primaryKeyColumns: pkCols,
	}
}

func getKeyValues(event *core.DBChangeEvent, cols []string) []interface{} {
	result := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		result = append(result, event.GetRow()[col])
	}
	return result
}

func genKeyHash(db string, table string, pks []interface{}) int {
	result := utils.GetFNV64aHash(db)
	result += utils.GetFNV64aHash(table)
	for _, pk := range pks {
		result += utils.GetFNV64aHash(fmt.Sprintf("%v", pk))
	}
	return utils.AbsInt(result)
}

func genTableHash(db string, table string) int {
	result := utils.GetFNV64aHash(db)
	result += utils.GetFNV64aHash(table)
	return utils.AbsInt(result)
}

func (o *MysqlStreamOutput) checkProgress() {
	ticker := time.NewTicker(utils.DefaultCheckProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for i, queue := range o.taskQueues {
				o.GetLogger().Info("MysqlStreamOutput task queue", log.Int("No.", i),
					log.Int("length", len(queue)))
			}
		case <-o.stopWaitContext.Done():
			return
		}
	}
}

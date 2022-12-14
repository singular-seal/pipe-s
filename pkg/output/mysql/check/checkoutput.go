package check

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
	"time"
)

const (
	DefaultFlushIntervalMS = 100
	DefaultFlushBatchSize  = 500
)

type MysqlCheckOutputConfig struct {
	ID       string
	Host     string
	Port     uint16
	User     string
	Password string

	OutputFilePath string
	// if we need skip checking for most recently updated rows, we can use UpdateTimeColumn and UpdateTimeSkipSeconds
	// to construct a query condition like 'UpdateTimeColumn<Now()-UpdateTimeSkipSeconds'
	UpdateTimeColumn      string
	UpdateTimeSkipSeconds int64
	TableBufferSize       int   // max messages buffered for each table
	TableFlushIntervalMS  int64 // max ms between each table buffer flushing
}

type MysqlCheckOutput struct {
	*core.BaseOutput
	config          *MysqlCheckOutputConfig
	tableProcessors map[string]*TableProcessor
	conn            *sql.DB
	schemaStore     schema.SchemaStore // the schema store to load table schemas

	stopWaitContext context.Context
	stopCancel      context.CancelFunc
}

func NewMysqlCheckOutput() *MysqlCheckOutput {
	ctx, cancelFunc := context.WithCancel(context.Background())

	output := &MysqlCheckOutput{
		BaseOutput:      core.NewBaseOutput(),
		tableProcessors: make(map[string]*TableProcessor),
		stopWaitContext: ctx,
		stopCancel:      cancelFunc,
	}
	return output
}

func (o *MysqlCheckOutput) Configure(config core.StringMap) (err error) {
	c := &MysqlCheckOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c

	if o.config.TableFlushIntervalMS == 0 {
		o.config.TableFlushIntervalMS = DefaultFlushIntervalMS
	}

	if o.config.TableBufferSize == 0 {
		o.config.TableBufferSize = DefaultFlushBatchSize
	}
	return nil
}

type TableProcessor struct {
	messages      chan *core.Message
	output        *MysqlCheckOutput
	tableSchema   *core.Table
	fullTableName string
	columnTypeMap map[string]*sql.ColumnType
	flushSig      chan bool
	lastFlushTime int64
	conn          *sql.DB
	stopContext   context.Context
	logger        *log.Logger
}

func NewTableProcessor(db string, table string, output *MysqlCheckOutput) (*TableProcessor, error) {
	ts, err := output.schemaStore.GetTable(db, table)
	if err != nil {
		return nil, err
	}
	columnTypes, err := utils.GetColumnTypes(db, table, ts.ColumnNames(), output.conn)
	if err != nil {
		return nil, err
	}
	typeMap := make(map[string]*sql.ColumnType)
	for i, col := range ts.ColumnNames() {
		typeMap[col] = columnTypes[i]
	}

	proc := &TableProcessor{
		output:        output,
		tableSchema:   ts,
		columnTypeMap: typeMap,
		fullTableName: utils.FullTableName(db, table),
		messages:      make(chan *core.Message, output.config.TableBufferSize),
		flushSig:      make(chan bool),
		conn:          output.conn,
		stopContext:   output.stopWaitContext,
		logger:        output.GetLogger(),
	}
	return proc, nil
}

func (p *TableProcessor) Run() {
	p.lastFlushTime = time.Now().UnixNano() / 1e6

	go func() {
		ticker := utils.IntervalCheckTicker(p.output.config.TableFlushIntervalMS)
		for {
			select {
			case <-p.stopContext.Done():
				p.logger.Info("processor exit", log.String("fullTableName", p.fullTableName))
				return
			case <-ticker.C:
				if time.Now().UnixNano()/1e6-p.lastFlushTime > p.output.config.TableFlushIntervalMS {
					p.Flush()
				}
			case <-p.flushSig:
				p.Flush()
			}
		}
	}()
}

func (p *TableProcessor) Flush() {
	p.lastFlushTime = time.Now().UnixNano() / 1e6
	size := len(p.messages)
	if size == 0 {
		return
	}

	messages := make([]*core.Message, 0)
	for i := 0; i < size; i++ {
		messages = append(messages, <-p.messages)
	}

	err := p.check(messages)
	for _, message := range messages {
		p.output.GetInput().Ack(message, err)
	}
}

func (p *TableProcessor) check(messages []*core.Message) error {
	selCols := messages[0].Data.(*core.DBChangeEvent).GetColumns()
	pkCols := p.tableSchema.PKColumnNames()
	sqlString, args := generateSqlAndArgs(p.tableSchema.DBName, p.tableSchema.TableName, pkCols, selCols, getPKValues(pkCols, messages))
	target, err := p.executeSelect(sqlString, args, selCols)
	if err != nil {
		return err
	}

	return nil
}

func (p *TableProcessor) executeSelect(sqlString string, args []interface{}, selCols []string) ([]map[string]interface{}, error) {
	// execute
	rows, err := p.conn.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	// put query result into placeholders
	colTypes := make([]*sql.ColumnType, 0)
	for _, col := range selCols {
		colTypes = append(colTypes, p.columnTypeMap[col])
	}
	batchDataPointers := utils.NewBatchDataPointers(colTypes, len(args))

	rowIdx := 0
	for rows.Next() {
		if batchDataPointers[rowIdx], err = utils.ScanRowsWithDataPointers(rows, colTypes, batchDataPointers[rowIdx]); err != nil {
			return nil, err
		}
		rowIdx++
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	// add column name to query result
	result := make([]map[string]interface{}, 0)
	// rowIdx is number of rows now
	for i := 0; i < rowIdx; i++ {
		rowKV := make(map[string]interface{})
		values := utils.ReadDataFromPointers(batchDataPointers[i])
		for idx, col := range selCols {
			rowKV[col] = values[idx]
		}
		result = append(result, rowKV)
	}
	return result, nil
}

func generateSqlAndArgs(db string, table string, selCols []string, pkCols []string, pkValues [][]interface{}) (string, []interface{}) {
	sqlPrefix := fmt.Sprintf("select %s from %s.%s where (%s) in", strings.Join(selCols, ","), db, table,
		strings.Join(pkCols, ","))

	batchPlaceHolders := make([]string, 0)
	batchArgs := make([]interface{}, 0)
	for _, pkValue := range pkValues {
		phs := make([]string, 0)
		for _, v := range pkValue {
			phs = append(phs, "?")
			batchArgs = append(batchArgs, v)
		}
		batchPlaceHolders = append(batchPlaceHolders, fmt.Sprintf("%s", strings.Join(phs, ",")))
	}
	s := []string{sqlPrefix, fmt.Sprintf("(%s)", strings.Join(batchPlaceHolders, ","))}
	return strings.Join(s, " "), batchArgs
}

func getPKValues(pkCols []string, messages []*core.Message) [][]interface{} {
	result := make([][]interface{}, 0)
	for _, msg := range messages {
		event := msg.Data.(*core.DBChangeEvent)
		pk := make([]interface{}, 0)
		for _, col := range pkCols {
			pk = append(pk, event.GetRow()[col])
		}
		result = append(result, pk)
	}
	return result
}

func (p *TableProcessor) Process(m *core.Message) {
	p.messages <- m
	if len(p.messages) >= p.output.config.TableBufferSize {
		p.flushSig <- true
	}
}

func (o *MysqlCheckOutput) Process(m *core.Message) {
	dbChange := m.Data.(*core.DBChangeEvent)
	ft := utils.FullTableName(dbChange.Database, dbChange.Table)
	p, ok := o.tableProcessors[ft]

	if !ok {
		p, err := NewTableProcessor(dbChange.Database, dbChange.Table, o)
		if err != nil {
			o.GetInput().Ack(m, err)
			return
		}
		p.Run()
	}
	p.Process(m)
}

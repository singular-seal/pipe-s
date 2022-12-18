package check

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	DefaultFlushIntervalMS = 100
	DefaultFlushBatchSize  = 500
)

const (
	UpdateTimeTypeTime  = "time"
	UpdateTimeTypeSec   = "sec"
	UpdateTimeTypeMilli = "milli"
	UpdateTimeTypeNano  = "nano"
)

const (
	ErrorTypeRowMiss = "row_miss"
	ErrorTypeRowDiff = "row_diff"
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
	// this function only works if the pk values are not modified in the pipeline because implementing a reverse pipeline
	// operation is cumbersome
	UpdateTimeColumn string
	// can be time,sec(second),milli(milli second) and nano(nano second
	UpdateTimeType        string
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

	resultReportingLock sync.Mutex
	stopWaitContext     context.Context
	stopCancel          context.CancelFunc
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
	if len(o.config.UpdateTimeType) == 0 {
		o.config.UpdateTimeType = UpdateTimeTypeTime
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
	sqlString, args := generateSelectSqlAndArgs(p.tableSchema.DBName, p.tableSchema.TableName, pkCols, selCols, getPKValues(pkCols, messages))
	target, err := p.executeSelect(sqlString, args, selCols)
	if err != nil {
		return err
	}
	diffItems, err := p.checkData(messages, target, pkCols)
	if err != nil {
		return err
	}
	if len(diffItems) > 0 {
		return p.reportResult(diffItems)
	}
	return nil
}

func (p *TableProcessor) reportResult(diffItems []*checkOutputItem) error {
	p.output.resultReportingLock.Lock()
	defer p.output.resultReportingLock.Unlock()

	if len(p.output.config.OutputFilePath) > 0 {
		f, err := os.OpenFile(p.output.config.OutputFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		defer f.Close()

		for _, item := range diffItems {
			if _, err = f.WriteString(item.String()); err != nil {
				return err
			}
		}
	}
	return nil
}

type checkOutputItem struct {
	DBName      string
	TableName   string
	ErrorType   string
	ExpectedRow map[string]interface{}
	RealRow     map[string]interface{}
}

func (i *checkOutputItem) String() string {
	t := fmt.Sprintf("err:%s db:%s table:%s ", i.ErrorType, i.DBName, i.TableName)
	pairs := make([]string, 0)
	for k, v := range i.ExpectedRow {
		pairs = append(pairs, fmt.Sprintf("%s:%v", k, v))
	}
	e := fmt.Sprintf("expected:%s", strings.Join(pairs, " "))
	pairs = make([]string, 0)
	for k, v := range i.RealRow {
		pairs = append(pairs, fmt.Sprintf("%s:%v", k, v))
	}
	r := fmt.Sprintf("real:%s", strings.Join(pairs, " "))
	return fmt.Sprintf("%s - %s %s", t, e, r)
}

func toPKMap(data []map[string]interface{}, pkCols []string) map[interface{}]map[string]interface{} {
	result := make(map[interface{}]map[string]interface{})
	if len(pkCols) == 1 {
		for _, v := range data {
			result[v[pkCols[0]]] = v
		}
	} else {
		for _, v := range data {
			pk := make([]interface{}, 0)
			for _, col := range pkCols {
				pk = append(pk, v[col])
			}
			result[pkString(pk)] = v
		}
	}
	return result
}

func pkString(values []interface{}) string {
	return fmt.Sprint(values...)
}

// pkValue returns a string composed of all values of pk columns for composite pk
func pkValue(data map[string]interface{}, pkCols []string) interface{} {
	if len(pkCols) == 1 {
		return data[pkCols[0]]
	} else {
		pks := make([]interface{}, 0)
		for _, col := range pkCols {
			pks = append(pks, data[col])
		}
		return pkString(pks)
	}
}

func (p *TableProcessor) checkData(sourceMessages []*core.Message, targetData []map[string]interface{},
	pkColumns []string) (diffItems []*checkOutputItem, err error) {

	misses := make([]*core.Message, 0)
	diffs := make([]*core.Message, 0)
	// find diffs
	targetPKMap := toPKMap(targetData, pkColumns)
	for _, message := range sourceMessages {
		event := message.Data.(*core.DBChangeEvent)
		targetRecord, ok := targetPKMap[pkValue(event.GetRow(), pkColumns)]
		if !ok {
			misses = append(misses, message)
			continue
		}
		if hasDiff(event.GetRow(), targetRecord) {
			diffs = append(diffs, message)
		}
	}
	// double check to exclude false diff
	if len(p.output.config.UpdateTimeColumn) > 0 {
		misses, err = p.recheckMissingRecords(misses)
		if err != nil {
			return nil, err
		}
		diffs, err = p.recheckDifferentRecords(diffs)
		if err != nil {
			return nil, err
		}
	}
	// check result
	for _, each := range misses {
		event := each.Data.(*core.DBChangeEvent)
		item := &checkOutputItem{
			DBName:      event.Database,
			TableName:   event.Table,
			ErrorType:   ErrorTypeRowMiss,
			ExpectedRow: event.GetRow(),
			RealRow:     nil,
		}
		diffItems = append(diffItems, item)
	}
	for _, each := range diffs {
		event := each.Data.(*core.DBChangeEvent)
		targetRecord, _ := targetPKMap[pkValue(event.GetRow(), pkColumns)]
		item := &checkOutputItem{
			DBName:      event.Database,
			TableName:   event.Table,
			ErrorType:   ErrorTypeRowDiff,
			ExpectedRow: event.GetRow(),
			RealRow:     targetRecord,
		}
		diffItems = append(diffItems, item)
	}
	return
}

func (p *TableProcessor) recheckMissingRecords(messages []*core.Message) ([]*core.Message, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	destPK := p.tableSchema.PKColumnNames()
	srcTable, ok := messages[0].GetTableSchema()
	if !ok {
		return nil, fmt.Errorf("no schema:%s", messages[0].Header.ID)
	}
	srcPK := srcTable.PKColumnNames()
	// try to filter the records deleted from source db recently
	sqlString, args := generateSelectSqlAndArgs(srcTable.DBName, srcTable.TableName, srcPK, srcPK, getPKValues(destPK, messages))
	data, err := p.executeSelect(sqlString, args, srcPK)
	if err != nil {
		return nil, err
	}
	result := make([]*core.Message, 0)
	if len(data) == 0 {
		return result, nil
	}

	srcPKMap := toPKMap(data, srcPK)
	for _, msg := range messages {
		event := msg.Data.(*core.DBChangeEvent)
		if _, ok := srcPKMap[pkValue(event.GetRow(), destPK)]; ok {
			result = append(result, msg)
		}
	}
	return result, nil
}

func (p *TableProcessor) recheckDifferentRecords(messages []*core.Message) ([]*core.Message, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	destPK := p.tableSchema.PKColumnNames()
	srcTable, ok := messages[0].GetTableSchema()
	if !ok {
		return nil, fmt.Errorf("no schema:%s", messages[0].Header.ID)
	}
	srcPK := srcTable.PKColumnNames()
	// try to filter the records deleted from source db recently
	sqlString, args := generateSelectSqlAndArgs(srcTable.DBName, srcTable.TableName, srcPK, srcPK, getPKValues(destPK, messages))
	w, arg := p.whereConditionForUpdateTime()
	sqlString = fmt.Sprintf("%s and %s", sqlString, w)
	args = append(args, arg)
	data, err := p.executeSelect(sqlString, args, srcPK)
	if err != nil {
		return nil, err
	}

	result := make([]*core.Message, 0)
	if len(data) == 0 {
		return result, nil
	}
	srcPKMap := toPKMap(data, srcPK)
	for _, msg := range messages {
		event := msg.Data.(*core.DBChangeEvent)
		if _, ok := srcPKMap[pkValue(event.GetRow(), destPK)]; ok {
			result = append(result, msg)
		}
	}
	return result, nil
}

func (p *TableProcessor) whereConditionForUpdateTime() (sql string, v interface{}) {
	sql = fmt.Sprintf("%s<?", p.output.config.UpdateTimeColumn)
	switch p.output.config.UpdateTimeType {
	case UpdateTimeTypeSec:
		v = time.Now().Add(time.Second * time.Duration(-p.output.config.UpdateTimeSkipSeconds)).Unix()
	case UpdateTimeTypeMilli:
		v = time.Now().Add(time.Second * time.Duration(-p.output.config.UpdateTimeSkipSeconds)).UnixMilli()
	case UpdateTimeTypeNano:
		v = time.Now().Add(time.Second * time.Duration(-p.output.config.UpdateTimeSkipSeconds)).UnixNano()
	default:
		v = time.Now().Add(time.Second * time.Duration(-p.output.config.UpdateTimeSkipSeconds))
	}
	return
}

func hasDiff(source map[string]interface{}, target map[string]interface{}) bool {
	for k, v := range source {
		_, ok := v.(sql.RawBytes)
		if ok {
			continue
		}
		_, ok = target[k].(sql.RawBytes)
		if ok {
			continue
		}

		if v != target[k] {
			return true
		}
	}
	return false
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

func generateSelectSqlAndArgs(db string, table string, selCols []string, pkCols []string, pkValues [][]interface{}) (string, []interface{}) {
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

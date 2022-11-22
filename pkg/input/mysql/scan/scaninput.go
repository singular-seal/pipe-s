package scan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultBatchSize   = 128
	DefaultConcurrency = 8
)

type MysqlScanInputConfig struct {
	Host           string
	Port           uint16
	User           string
	Password       string
	TableNameRegex string // select tables need to be replicated by regex
	BatchSize      int
	Concurrency    int
}

type MysqlScanInput struct {
	*core.BaseInput
	Config       *MysqlScanInputConfig
	dbConnection *sql.DB
	schemaStore  schema.SchemaStore
	scanState    *sync.Map
	// last message sequence of each table processed
	lastMsgSequences *sync.Map
	tableScanners    []*TableScanner // table scanners in this stream
	currentSequence  uint64
	stopWaitContext  context.Context
	stopCancel       context.CancelFunc
	lastAckError     atomic.Value // the last acknowledged error received
}

type TableScanner struct {
	id        int
	input     *MysqlScanInput
	batchSize int
	tables    chan *core.Table
	scanState *sync.Map
	logger    *log.Logger
	hasError  bool
}

func NewTableScanner(id int, input *MysqlScanInput, tables chan *core.Table) *TableScanner {
	scanner := &TableScanner{
		id:        id,
		input:     input,
		batchSize: input.Config.BatchSize,
		tables:    tables,
		scanState: input.scanState,
		logger:    input.GetLogger(),
		hasError:  false,
	}
	return scanner
}

func (scanner *TableScanner) start() error {
	go func() {
		for {
			select {
			case table, ok := <-scanner.tables:
				if !ok {
					return
				}
				err := scanner.scanTable(table)
				if err != nil {
					scanner.logger.Error("failed scan table", log.String("db", table.DBName),
						log.String("table", table.TableName), log.Error(err))
					scanner.input.RaiseError(err)
					return
				}
			case <-scanner.input.stopWaitContext.Done():
				scanner.logger.Info("table scan stopped", log.Int("id", scanner.id))
				return
			}
		}
	}()

	return nil
}

func NewMysqlScanInput() *MysqlScanInput {
	ctx, function := context.WithCancel(context.Background())

	return &MysqlScanInput{
		BaseInput:        core.NewBaseInput(),
		tableScanners:    make([]*TableScanner, 0),
		scanState:        &sync.Map{},
		lastMsgSequences: &sync.Map{},
		stopWaitContext:  ctx,
		stopCancel:       function,
	}
}

func (in *MysqlScanInput) Configure(config core.StringMap) (err error) {
	c := &MysqlScanInputConfig{}

	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	in.Config = c

	if in.Config.BatchSize == 0 {
		in.Config.BatchSize = DefaultBatchSize
	}
	if in.Config.Concurrency == 0 {
		in.Config.Concurrency = DefaultConcurrency
	}
	return
}

func (in *MysqlScanInput) Start() (err error) {
	if in.dbConnection, err = utils.CreateMysqlClient(in.Config.Host, in.Config.Port, in.Config.User, in.Config.Password); err != nil {
		return
	}

	in.schemaStore = schema.NewSimpleSchemaStoreWithClient(in.dbConnection)

	var tables chan *core.Table
	if tables, err = in.getTables(); err != nil {
		return
	}

	for i := 0; i < in.Config.Concurrency; i++ {
		scanner := NewTableScanner(i, in, tables)
		in.tableScanners = append(in.tableScanners, scanner)
		err = scanner.start()
		if err != nil {
			return
		}
	}
	return
}

func (in *MysqlScanInput) getTables() (tables chan *core.Table, err error) {
	sqlStr := "select distinct table_schema, table_name from information_schema.tables " +
		"where table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys') and table_type = 'BASE TABLE'"
	var rows *sql.Rows
	if rows, err = in.dbConnection.Query(sqlStr); err != nil {
		return
	}

	tabCount := 0
	tabs := make([][2]string, 0)
	tableReg := regexp.MustCompile(in.Config.TableNameRegex)
	for rows.Next() {
		var dbName, tableName string
		if err = rows.Scan(&dbName, &tableName); err != nil {
			return
		}

		if tableReg.MatchString(fmt.Sprintf("%s.%s", dbName, tableName)) {
			tabCount++
			tabs = append(tabs, [2]string{dbName, tableName})
		}
	}

	if err = rows.Err(); err != nil {
		return
	}
	if err = rows.Close(); err != nil {
		return
	}
	for _, each := range tabs {
		in.GetLogger().Info("scan table", log.String("", fmt.Sprintf("%s.%s", each[0], each[1])))
	}

	workChan := make(chan *core.Table, tabCount)
	for _, tab := range tabs {
		var ts *core.Table
		if ts, err = in.schemaStore.GetTable(tab[0], tab[1]); err != nil {
			return
		}
		workChan <- ts
	}
	return workChan, nil
}

func (in *MysqlScanInput) newDMLMessage(row []interface{}, pkVal []interface{}, createTime uint64, table *core.Table) *core.Message {
	m := core.NewMessage(core.TypeDML)
	m.Header.CreateTime = createTime
	m.Header.Sequence = atomic.AddUint64(&in.currentSequence, 1)
	// consider add pk as message id

	dml := &core.MysqlDMLEvent{
		FullTableName: table.DBName + "." + table.TableName,
		Operation:     core.DBInsert,
		NewRow:        row,
	}
	m.SetMeta(core.MetaTableSchema, table)
	m.SetMeta(core.MetaMySqlScanPos, pkVal)
	m.Data = dml
	return m
}

func (in *MysqlScanInput) Ack(msg *core.Message, err error) {
	if err != nil {
		in.lastAckError.Store(err)
		return
	}
	ts, _ := msg.GetTableSchema()
	key := [2]string{ts.DBName, ts.TableName}
	obj, _ := in.scanState.Load(key)
	tableState := obj.(*TableState)
	cs, _ := msg.GetMeta(core.MetaMySqlScanPos)
	tableState.ColumnStates.Store(cs)
	atomic.AddInt64(&tableState.FinishedCount, 1)

	seqObj, ok := in.lastMsgSequences.Load(key)
	if ok {
		lastSeq := seqObj.(uint64)
		// after last message ack, set tableState as done
		if msg.Header.Sequence == lastSeq {
			tableState.Done = true
			in.lastMsgSequences.Delete(key)
		}
	}
}

// todo implent it here
func (in *MysqlScanInput) GetState() ([]byte, bool) {
	done := true
	//SyncMap can't be marshalled by json, so transfer to a normal map.
	state := &MysqlDumpTaskState{
		Progress: make(map[string]*TableState),
		Error:    "",
	}
	s.batchState.Range(func(key, value interface{}) bool {
		tableState := value.(*TableState)
		tableKey := key.(core.TableKey)
		//todo sometimes there is dirty empty key, didn't find where it comes, should investigate
		if len(tableKey.Database) == 0 || len(tableKey.Table) == 0 {
			return true
		}
		if !tableState.Done {
			done = false
		}

		state.Progress[tableKey.ToString()] = tableState
		return true
	})

	if done {
		s.Logger.Info("all_tasks_done")
	}

	if s.lastError != nil {
		s.Logger.WithError(s.lastError).Error("task_has_error")
		state.Error = s.lastError.Error()
		done = true
	}

	result, err := json.Marshal(state)
	if err != nil {
		s.Logger.WithError(err).WithField("state", s.batchState).
			Error("marshal_batch_state_error")

	}
	return result, done
}

func (scanner *TableScanner) scanTable(table *core.Table) (err error) {
	stateObj, ok := scanner.scanState.Load([2]string{table.DBName, table.TableName})
	var tableState *TableState
	if !ok {
		tableState = &TableState{}
		if err = InitTableState(tableState, table, scanner.input.dbConnection); err != nil {
			return
		}
		scanner.scanState.Store([2]string{table.DBName, table.TableName}, tableState)
	} else {
		tableState = stateObj.(*TableState)
	}

	if tableState.Done {
		scanner.logger.Info("already finished", log.String("db", table.DBName),
			log.String("table", table.TableName))
		return
	}
	// make batch data placeholders
	columnTypes, err := utils.GetColumnTypes(table.DBName, table.TableName, nil, scanner.input.dbConnection)
	if err != nil {
		return
	}
	batchDataPointers := utils.NewBatchDataPointers(columnTypes, scanner.batchSize+1)

	scanner.logger.Info("start scan", log.String("db", table.DBName),
		log.String("table", table.TableName), log.Int64("total", tableState.EstimatedCount), log.Int64("start", tableState.FinishedCount))

	minValue := tableState.ColumnStates.Load().([]interface{})
	// pivotIndex starts with the right most index column will change in loop
	pivotIndex := len(table.PKColumns) - 1

	for {
		// scan table, it scans BatchSize+1 records in which the last record helps use to locate the start of next batch and the last batch
		statement, args := scanner.generateScanSqlAndArgs(table, table.PKColumnNames(), pivotIndex, minValue, scanner.batchSize+1)
		scanner.logger.Info("start scan", log.String("db", table.DBName), log.String("table", table.TableName),
			log.String("pk", fmt.Sprint(minValue)))

		var rows *sql.Rows
		if rows, err = scanner.input.dbConnection.Query(statement, args...); err != nil {
			return
		}

		rowIdx := 0
		for rows.Next() {
			if batchDataPointers[rowIdx], err = utils.ScanRowsWithDataPointers(rows, columnTypes, batchDataPointers[rowIdx]); err != nil {
				return
			}
			rowIdx++
		}
		if err = rows.Err(); err != nil {
			return
		}
		if err = rows.Close(); err != nil {
			return
		}

		// didn't find enough rows, so it is the last batch
		lastBatch := rowIdx < scanner.batchSize+1
		if !lastBatch {
			pivotIndex, err = findPivot(
				utils.ReadDataFromPointers(batchDataPointers[scanner.batchSize-1]),
				utils.ReadDataFromPointers(batchDataPointers[scanner.batchSize]),
			)
			if err != nil {
				return
			}
		}

		//fire messages
		for i := 0; i < rowIdx; i++ {
			rowVal := utils.ReadDataFromPointers(batchDataPointers[i])
			pkVal := getPKValue(rowVal, table)
			// this is the locating row, won't generate new message
			if i == scanner.batchSize {
				minValue = pkVal
				continue
			}

			msg := scanner.input.newDMLMessage(rowVal, pkVal, uint64(time.Now().UnixNano()), table)
			if lastBatch && i == rowIdx-1 {
				scanner.input.lastMsgSequences.Store([2]string{table.DBName, table.TableName}, msg.Header.Sequence)
			}
			scanner.input.GetOutput().Process(msg)
		}

		if lastBatch {
			scanner.logger.Info("finished scan table", log.String("db", table.DBName), log.String("table", table.TableName))
			return
		}
	}
}

func getPKValue(row []interface{}, table *core.Table) []interface{} {
	r := make([]interface{}, len(table.PKColumns))
	for i := 0; i < len(table.PKColumns); i++ {
		r[i] = row[table.PKColumns[i].Index]
	}
	return r
}

func findPivot(last []interface{}, next []interface{}) (int, error) {
	if len(last) != len(next) {
		return 0, fmt.Errorf("column count not match %d:%d", len(last), len(next))
	}
	for i := 0; i < len(last); i++ {
		if last[i] != next[i] {
			return i, nil
		}
	}
	return 0, fmt.Errorf("different rows have same pk")
}

// pivotIndex is an index in scanColumns for where condition generation
func (scanner *TableScanner) generateScanSqlAndArgs(
	table *core.Table,
	scanColumns []string,
	pivotIndex int,
	minValue []interface{},
	batch int) (string, []interface{}) {

	prefix := fmt.Sprintf("select * from `%s`.`%s` where ", table.DBName, table.TableName)

	var args []interface{}
	var where []string

	for i := 0; i <= pivotIndex-1; i++ {
		where = append(where, fmt.Sprintf("%s = ?", scanColumns[i]))
		args = append(args, minValue[i])
	}

	where = append(where, fmt.Sprintf("%s >= ?", scanColumns[pivotIndex]))
	args = append(args, minValue[pivotIndex])

	whereString := strings.Join(where, " and ")
	orderByString := strings.Join(scanColumns, ", ")

	query := fmt.Sprintf("%s%s order by %s limit ?", prefix, whereString, orderByString)
	args = append(args, batch)
	return query, args
}

func (in *MysqlScanInput) Stop() {
	in.stopCancel()
	in.schemaStore.Close()
}

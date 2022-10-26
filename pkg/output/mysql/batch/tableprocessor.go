package batch

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
	"sync"
	"time"
)

const DefaultInChanSize = 100
const DefaultFlushChanSize = 20

type MessageInfo struct {
	key      *[DefaultMaxComboKeyColumns]interface{} // unique key of the message
	dbChange *core.DBChangeEvent
	message  *core.Message
}

type TableProcessor struct {
	index           int
	output          *MysqlBatchOutput
	collectingBatch *BatchMessage
	inChan          chan *MessageInfo

	lastFlushTime time.Time
	flushChan     chan *BatchMessage
	conn          *sql.DB
	flushWait     *sync.WaitGroup // for insert, update and delete concurrency control
	stopContext   context.Context
	logger        *log.Logger
}

func NewTableProcessor(output *MysqlBatchOutput, index int) *TableProcessor {
	proc := &TableProcessor{
		index:           index,
		output:          output,
		collectingBatch: NewBatchMessage(),
		inChan:          make(chan *MessageInfo, DefaultInChanSize),
		flushChan:       make(chan *BatchMessage, DefaultFlushChanSize),
		conn:            output.conn,
		flushWait:       &sync.WaitGroup{},
		logger:          output.GetLogger(),
	}
	return proc
}

func (p *TableProcessor) Run() {
	// flushing and collecting messages in different goroutines
	go func() {
		for {
			select {
			case <-p.stopContext.Done():
				p.logger.Info("flush goroutine exited")
				return
			case batch := <-p.flushChan:
				p.flush(batch)
			}
		}
	}()

	go func() {
		p.lastFlushTime = time.Now()
		ticker := time.NewTicker(time.Millisecond * time.Duration(p.output.config.FlushIntervalMS/10))
		for {
			select {
			case <-p.stopContext.Done():
				p.logger.Info("message processing goroutine exited")
				return
			case <-ticker.C:
				if time.Since(p.lastFlushTime).Milliseconds() > p.output.config.FlushIntervalMS {
					p.sendFlush()
				}
			case msg := <-p.inChan:
				if err := p.collectingBatch.add(msg); err != nil {
					p.logger.Error("wrong message sequence found", log.Error(err))
					p.ack([]*core.Message{msg.message}, err)
					return
				}
				if p.collectingBatch.size >= p.output.config.FlushBatchSize {
					p.sendFlush()
				}
			}
		}
	}()
}

func copyKey(key []interface{}) *[DefaultMaxComboKeyColumns]interface{} {
	var result [DefaultMaxComboKeyColumns]interface{}
	for i, each := range key {
		result[i] = each
	}
	return &result
}

func (p *TableProcessor) Process(key []interface{}, dbEvent *core.DBChangeEvent, msg *core.Message) {
	p.inChan <- &MessageInfo{
		key:      copyKey(key),
		dbChange: dbEvent,
		message:  msg,
	}
}

func (p *TableProcessor) executeBatch(messages []*MergedMessage) {
	messages = p.filter(messages)
	if len(messages) == 0 {
		return
	}
	var current []*MergedMessage
	for len(messages) > 0 {
		if len(messages) > p.output.config.SqlBatchSize {
			current = messages[:p.output.config.SqlBatchSize]
			messages = messages[p.output.config.SqlBatchSize:]
		} else {
			current = messages
			messages = []*MergedMessage{}
		}
		err := p.executeSome(current)
		for _, each := range current {
			p.ack(each.originals, err)
		}
	}
	return
}

func (p *TableProcessor) executeSome(messages []*MergedMessage) error {
	sqlString, sqlArgs := p.generateSql(messages)
	if len(sqlString) == 0 {
		return fmt.Errorf("blank sql")
	}
	result, err := p.conn.Exec(sqlString, sqlArgs...)
	if err != nil {
		p.logger.Error("failed execute sql", log.String("sql", sqlString), log.Error(err))
		return err
	}
	count, err := result.RowsAffected()
	// this can happen in retry so just log it
	if count < int64(len(messages)) {
		p.logger.Warn("not all rows succeed", log.String("sql", sqlString),
			log.Int64("succeed", count), log.Int("all", len(messages)))
	}
	return err
}

func (p *TableProcessor) generateSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	switch messages[0].mergedEvent.Operation {
	case core.DBInsert:
		return p.generateInsertOrReplaceSql(messages)
	case core.DBUpdate:
		return p.generateUpdateSql(messages)
	case core.DBDelete:
		return p.generateDeleteSql(messages)
	case core.DBReplace:
		return p.generateInsertOrReplaceSql(messages)
	}
	return "", nil
}

func columnValues(event *core.DBChangeEvent, columns []string) []interface{} {
	result := make([]interface{}, 0)
	for _, column := range columns {
		result = append(result, event.GetRow()[column])
	}
	return result
}

func (p *TableProcessor) generateInsertOrReplaceSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	msg0 := messages[0]
	columns := msg0.originals[0].ColumnNames()
	var sqlPrefix string
	if msg0.mergedEvent.Operation == core.DBReplace {
		sqlPrefix = fmt.Sprintf("replace into %s.%s (%s) values", msg0.mergedEvent.Database,
			msg0.mergedEvent.Table, strings.Join(utils.QuoteColumns(columns), ","))
	} else {
		sqlPrefix = fmt.Sprintf("insert ignore into %s.%s (%s) values", msg0.mergedEvent.Database,
			msg0.mergedEvent.Table, strings.Join(utils.QuoteColumns(columns), ","))
	}
	allPlaceHolders := make([]string, 0)
	allArgs := make([]interface{}, 0)

	for _, message := range messages {
		for _, val := range columnValues(message.mergedEvent, columns) {
			allArgs = append(allArgs, val)
		}
		phs := make([]string, 0)
		for i := 0; i < len(columns); i++ {
			phs = append(phs, "?")
		}
		rph := fmt.Sprintf("(%s)", strings.Join(phs, ","))
		allPlaceHolders = append(allPlaceHolders, rph)
	}

	placeHolderString := strings.Join(allPlaceHolders, ",")
	s := []string{sqlPrefix, placeHolderString}
	return strings.Join(s, " "), allArgs
}

func (p *TableProcessor) generateUpdateSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	var batchSql []string
	ts, _ := messages[0].originals[0].GetTableSchema()
	keyColumns := make([]string, len(ts.PKColumns))
	for i, column := range ts.PKColumns {
		keyColumns[i] = column.Name
	}

	for _, message := range messages {
		s, as := utils.GenerateSqlAndArgs(message.mergedEvent, keyColumns)
		batchSql = append(batchSql, s)
		sqlArgs = append(sqlArgs, as...)
	}
	sqlString = strings.Join(batchSql, ";")
	return
}

func (p *TableProcessor) generateDeleteSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	sqlString = fmt.Sprintf("delete from %s.%s where %s", messages[0].mergedEvent.Database,
		messages[0].mergedEvent.Table, genPKColumnsIn(messages))
	ts, _ := messages[0].originals[0].GetTableSchema()
	for _, column := range ts.PKColumns {
		for _, message := range messages {
			sqlArgs = append(sqlArgs, message.mergedEvent.OldRow[column.Name])
		}
	}
	return
}

func genPKColumnsIn(messages []*MergedMessage) string {
	ts, _ := messages[0].originals[0].GetTableSchema()
	conditions := make([]string, 0)
	for _, column := range ts.PKColumns {
		phs := make([]string, len(messages))
		for i := range phs {
			phs[i] = "?"
		}
		condition := fmt.Sprintf("%s in (%s)", column.Name, strings.Join(phs, ","))
		conditions = append(conditions, condition)
	}
	return strings.Join(conditions, " and ")
}

func (p *TableProcessor) filter(messages []*MergedMessage) []*MergedMessage {
	if len(messages) == 0 {
		return messages
	}
	if messages[0].mergedEvent.Operation != core.DBDelete {
		return messages
	}
	result := make([]*MergedMessage, 0)
	for _, message := range messages {
		if message.inDB {
			result = append(result, message)
		} else {
			// this is a INSERT-%-DELETE sequence, need not execute it
			p.ack(message.originals, nil)
		}
	}
	return result
}

func (p *TableProcessor) sendFlush() {
	p.lastFlushTime = time.Now()
	snapshot := p.collectingBatch.snapshot()
	if snapshot.size == 0 {
		return
	}
	p.flushChan <- snapshot
}

func (p *TableProcessor) flush(batchMessage *BatchMessage) {
	batches := batchMessage.splitByOperation()

	if p.output.config.ExecCRUDConcurrentlyInBatch {
		for _, each := range batches {
			p.flushWait.Add(1)
			go func(batch []*MergedMessage) {
				defer p.flushWait.Done()
				p.executeBatch(batch)
			}(each)
		}
		p.flushWait.Wait()
	} else {
		for _, each := range batches {
			p.executeBatch(each)
		}
	}
}

func (p *TableProcessor) ack(messages []*core.Message, err error) {
	for _, message := range messages {
		p.output.GetInput().Ack(message, err)
	}
}
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

type MessageInfo struct {
	key      [...]interface{} // unique key of the message
	dbChange *core.DBChangeEvent
	message  *core.Message
}

type TableProcessor struct {
	index           int
	output          *MysqlBatchOutput
	collectingBatch *BatchMessage
	messages        chan *MessageInfo

	lastFlushTime   int64
	toFlush         chan *BatchMessage
	batchCollectors map[string]*BatchCollector
	conn            *sql.DB
	flushWait       *sync.WaitGroup // for insert, update and delete concurrency control
	stopWaitContext context.Context
	logger          log.Logger
}

func NewTableProcessor(output *MysqlBatchOutput, index int) *TableProcessor {
	proc := &TableProcessor{
		index:           index,
		output:          output,
		collectingBatch: NewBatchMessage(),
		messages:        make(chan *MessageInfo),
		toFlush:         make(chan *BatchMessage),
		batchCollectors: make(map[string]*BatchCollector),
		conn:            output.conn,
		flushWait:       &sync.WaitGroup{},
	}
	return proc
}

func (p *TableProcessor) Run() {
	// flushing and collecting messages in different goroutines
	go func() {
		for toProcess := range p.toFlush {
			p.flush(toProcess)
		}
	}()

	go func() {
		p.lastFlushTime = time.Now().UnixNano() / 1e6
		ticker := time.NewTicker(time.Millisecond * time.Duration(p.output.config.FlushIntervalMS))
		for {
			select {
			case <-p.stopWaitContext.Done():
				p.logger.Info("table processor stopped")
				return
			case <-ticker.C:
				if time.Now().UnixNano()/1e6-p.lastFlushTime > p.output.config.FlushIntervalMS {
					p.sendFlush()
				}
			case msg := <-p.messages:
				p.collectingBatch.add(msg)
				if p.collectingBatch.size >= p.output.config.FlushBatchSize {
					p.sendFlush()
				}
			}
		}
	}()
}

func (p *TableProcessor) Process(key [...]interface{}, dbEvent *core.DBChangeEvent, msg *core.Message) {
	p.messages <- &MessageInfo{
		key:      key,
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
	real, err := result.RowsAffected()
	// this can happen in retry so just log it
	if real < int64(len(messages)) {
		p.logger.Warn("not all rows succeed", log.String("sql", sqlString),
			log.Int64("succeed", real), log.Int("all", len(messages)))
	}
	return err
}

func (p *TableProcessor) generateSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	switch messages[0].mergedEvent.Operation {
	case core.DBInsert:
		return p.generateInsertSql(messages)
	case core.DBUpdate:
		return p.generateUpdateSql(messages)
	case core.DBDelete:
		return p.generateDeleteSql(messages)
	case core.DBReplace:
		return p.generateReplaceSql(messages)
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

func (p *TableProcessor) generateInsertSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	msg0 := messages[0]
	columns := msg0.originals[0].ColumnNames()
	sqlPrefix := fmt.Sprintf("insert ignore into `%s`.`%s` (%s) values", msg0.mergedEvent.Database,
		msg0.mergedEvent.Table, strings.Join(utils.QuoteColumns(columns), ","))
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
	return "", nil
}
func (p *TableProcessor) generateDeleteSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	return "", nil
}
func (p *TableProcessor) generateReplaceSql(messages []*MergedMessage) (sqlString string, sqlArgs []interface{}) {
	return "", nil
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
	p.lastFlushTime = time.Now().UnixNano() / 1e6
	snapshot := p.collectingBatch.snapshot()
	if snapshot.size == 0 {
		return
	}
	p.toFlush <- snapshot
}

func (p *TableProcessor) flush(batchMessage *BatchMessage) {
	batches := batchMessage.splitByOperation()

	if p.output.config.ExecCRUDConcurrentlyInBatch {
		for _, batch := range batches {
			p.flushWait.Add(1)
			go func() {
				defer p.flushWait.Done()
				p.executeBatch(batch)
			}()
		}
		p.flushWait.Wait()
	} else {
		for _, batch := range batches {
			p.executeBatch(batch)
		}
	}
}

func (p *TableProcessor) ack(msg []*core.Message, err error) {
	for _, meta := range msg {
		p.output.GetInput().Ack(meta, err)
	}
}

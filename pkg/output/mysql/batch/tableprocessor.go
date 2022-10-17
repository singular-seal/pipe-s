package batch

import (
	"context"
	"database/sql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"sync"
	"time"
)

type MessageInfo struct {
	key      []interface{} // unique key of the message
	dbChange *core.DBChangeEvent
	header   *core.MessageHeader
}

type TableProcessor struct {
	index             int
	output            *MysqlBatchOutput
	messageCollection *MessageCollection
	messages          chan *MessageInfo

	lastFlushTime   int64
	toFlush         chan EventMap
	batchCollectors map[string]*BatchCollector
	dbClient        *sql.DB
	flushWait       *sync.WaitGroup // for insert, update and delete concurrency control
	stopWaitContext context.Context
	logger          log.Logger
}

func NewTableProcessor(output *MysqlBatchOutput, index int) *TableProcessor {
	proc := &TableProcessor{
		index:             index,
		output:            output,
		messageCollection: NewMessageCollection(),
		messages:          make(chan *MessageInfo),
		toFlush:           make(chan EventMap),
		batchCollectors:   make(map[string]*BatchCollector),
		dbClient:          output.conn,
		flushWait:         &sync.WaitGroup{},
	}

	proc.batchCollectors[core.MySQLCommandInsert] = NewBatchCollector(proc, core.MySQLCommandInsert)
	proc.batchCollectors[core.MySQLCommandUpdate] = NewBatchCollector(proc, core.MySQLCommandUpdate)
	proc.batchCollectors[core.MySQLCommandDelete] = NewBatchCollector(proc, core.MySQLCommandDelete)

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
				p.messageCollection.addToBatch(msg.pk, msg.dbChange, msg.header)
				if len(p.messageCollection.events) >= p.output.config.FlushBatchSize {
					p.sendFlush()
				}
			}
		}
	}()
}

func (p *TableProcessor) Process(key []interface{}, dbEvent *core.DBChangeEvent, header *core.MessageHeader) {
	p.messages <- &MessageInfo{
		key:      key,
		dbChange: dbEvent,
		header:   header,
	}
}

func (p *TableProcessor) sendFlush() {
	p.lastFlushTime = time.Now().UnixNano() / 1e6
	toProcess := p.messageCollection.getSnapshot()
	if len(toProcess) == 0 {
		return
	}
	p.toFlush <- toProcess
}

func (p *TableProcessor) flush(toProcess EventMap) {
	//p.logger.WithField("batch_size", len(toProcess)).WithField("proc_index", p.index).Info("longmysqlosprocessor_flushing")
	for pk, dataMessage := range toProcess {
		if dataMessage.dbEvent.Command == core.MySQLCommandDelete && dataMessage.existsInDb == false {
			// a sequence like INSERT-%-DELETE was encountered, need not execute it
			p.helpAck(dataMessage.toAck, nil)
			continue
		}

		sqlCommand := &core.SQLCommand{}
		utils.CopyDBEventToSQLCommand(dataMessage.dbEvent, sqlCommand, p.keyColumns)
		p.batchCollectors[dataMessage.dbEvent.Command].AddMessage(pk, sqlCommand, dataMessage.toAck)
	}

	ops := []string{
		core.MySQLCommandDelete,
		core.MySQLCommandInsert,
		core.MySQLCommandUpdate,
	}

	for _, op := range ops {
		p.flushWait.Add(1)
		go func(o string) {
			defer p.flushWait.Done()
			p.batchCollectors[o].Flush()
		}(op)
	}
	p.flushWait.Wait()

}

func (p *TableProcessor) helpAck(toAck []*core.MessageMeta, err error) {
	for _, meta := range toAck {
		p.output.UpStream.AckMessage(meta, err)
	}
}

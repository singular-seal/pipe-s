package batch

import (
	"context"
	"database/sql"
	"github.com/singular-seal/pipe-s/pkg/core"
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

	lastDumpTime int64

	flushSigChan chan bool // flushSigChan is signalled when flush batch size is reached

	toFlush         chan EventMap
	batchCollectors map[string]*BatchCollector
	keyColumns      []string
	dbClient        *sql.DB
	flushWait       *sync.WaitGroup // for insert, update and delete concurrency control

	ctx context.Context // stop output context
}

func NewTableProcessor(os *LongMysqlOutStream, index int) *TableProcessor {
	proc := &TableProcessor{
		index:             index,
		output:            os,
		messageCollection: NewMessageCollection(),
		messages:          make(chan *MessageInfo),
		flushSigChan:      make(chan bool),
		toFlush:           make(chan EventMap),
		batchCollectors:   make(map[string]*BatchCollector),
		keyColumns:        os.config.KeyColumns,
		dbClient:          os.dbClient,
		flushWait:         &sync.WaitGroup{},

		ctx:    os.ctx,
		logger: os.logger,
	}

	proc.batchCollectors[core.MySQLCommandInsert] = NewBatchCollector(proc, core.MySQLCommandInsert)
	proc.batchCollectors[core.MySQLCommandUpdate] = NewBatchCollector(proc, core.MySQLCommandUpdate)
	proc.batchCollectors[core.MySQLCommandDelete] = NewBatchCollector(proc, core.MySQLCommandDelete)

	return proc
}

func (p *TableProcessor) Run() {
	// listen incoming
	go func() {
		for msg := range p.messages {
			p.messageCollection.addToBatch(msg.pk, msg.dbChange, msg.header)
			if len(p.messageCollection.events) >= p.output.config.FlushBatchSize {
				p.flushSigChan <- true
			}
		}
	}()

	// listen flush
	// ensures the same pk will be processed in order
	go func() {
		for toProcess := range p.toFlush {
			p.ProcessFlush(toProcess)
		}
	}()

	p.lastDumpTime = time.Now().UnixNano() / 1e6
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(p.output.config.FlushIntervalMS))
		for {
			select {
			case <-p.ctx.Done():
				p.logger.Info("processor_terminated")
				return
			case <-ticker.C:
				if time.Now().UnixNano()/1e6-p.lastDumpTime > p.output.config.FlushIntervalMS {
					p.Flush()
				}
			case <-p.flushSigChan:
				p.Flush()
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

func (p *TableProcessor) Flush() {
	p.lastDumpTime = time.Now().UnixNano() / 1e6
	toProcess := p.messageCollection.getSnapshot()
	if len(toProcess) == 0 {
		return
	}
	p.toFlush <- toProcess
}

func (p *TableProcessor) ProcessFlush(toProcess EventMap) {
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
		// gives a more stable qps to count only after it returned from db
		metrics.Metrics.AddSentEvent("output")
	}
}

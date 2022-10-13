package batch

import (
	"context"
	"database/sql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"sync"
	"time"
)

type IncomingMessage struct {
	pk      interface{}
	dbEvent *core.DBChangeEvent
	meta    *core.MessageMeta
}

// TableProcessor is in-charge of one db/table
type TableProcessor struct {
	partition         int
	outstream         *LongMysqlOutStream
	messageCollection *MessageCollection
	incomingMessages  chan *IncomingMessage

	lastDumpTime int64

	flushSigChan chan bool // flushSigChan is signalled when flush batch size is reached

	toFlush         chan EventMap
	batchCollectors map[string]*BatchCollector
	keyColumns      []string
	dbClient        *sql.DB
	flushWait       *sync.WaitGroup // for insert, update and delete concurrency control

	ctx    context.Context // stop outstream context
	logger logger.ILogger
}

func NewLongMysqlOutStreamProcessor(os *LongMysqlOutStream, partition int) *TableProcessor {
	proc := &TableProcessor{
		partition:         partition,
		outstream:         os,
		messageCollection: NewMessageCollection(),
		incomingMessages:  make(chan *IncomingMessage),
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
		for msg := range p.incomingMessages {
			p.messageCollection.addToBatch(msg.pk, msg.dbEvent, msg.meta)
			if len(p.messageCollection.events) >= p.outstream.config.FlushBatchSize {
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
		ticker := time.NewTicker(time.Millisecond * time.Duration(p.outstream.config.FlushIntervalMS))
		for {
			select {
			case <-p.ctx.Done():
				p.logger.Info("processor_terminated")
				return
			case <-ticker.C:
				if time.Now().UnixNano()/1e6-p.lastDumpTime > p.outstream.config.FlushIntervalMS {
					p.Flush()
				}
			case <-p.flushSigChan:
				p.Flush()
			}
		}
	}()
}

func (p *TableProcessor) AddToBatch(pk interface{}, dbEvent *core.DBChangeEvent, meta *core.MessageMeta) {
	p.incomingMessages <- &IncomingMessage{
		pk:      pk,
		dbEvent: dbEvent,
		meta:    meta,
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
	//p.logger.WithField("batch_size", len(toProcess)).WithField("proc_index", p.partition).Info("longmysqlosprocessor_flushing")
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
		p.outstream.UpStream.AckMessage(meta, err)
		// gives a more stable qps to count only after it returned from db
		metrics.Metrics.AddSentEvent("outstream")
	}
}

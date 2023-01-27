package simple

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/metrics"
	"github.com/singular-seal/pipe-s/pkg/pipeline"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"github.com/smartystreets-prototypes/go-disruptor"
	"sync"
	"time"
)

const (
	DefaultDisruptorBufferSize   = 8192
	DefaultDisruptorConcurrency  = 4
	DefaultCheckProgressInterval = time.Second * 30
)

type DisruptorPipelineConfig struct {
	Concurrency int
	BufferSize  int
}

func NewDisruptorPipeline() *DisruptorPipeline {
	p := &DisruptorPipeline{
		BasePipeline: pipeline.NewBasePipeline(),
	}
	p.BasePipeline.LifeCycleFunctor = p
	return p
}

// DisruptorPipeline gets events from input and sends them to output through disruptor.
type DisruptorPipeline struct {
	*pipeline.BasePipeline
	config        *DisruptorPipelineConfig
	disruptor     disruptor.Disruptor
	ringBuffer    []*DisruptorEvent
	bufferMask    int64
	inQueueCount  uint64 // +1 when receives a msg
	outQueueCount uint64 // +1 when a msg is acked
	stop          bool
}

type DisruptorEvent struct {
	msg  *core.Message
	skip bool
	err  error
}

type ProcessConsumer struct {
	num             int64
	concurrencyMask int64
	pipeline        *DisruptorPipeline
}

func (c ProcessConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		if lower&c.concurrencyMask != c.num {
			continue
		}
		metrics.AddEventCount()
		event := c.pipeline.ringBuffer[lower&c.pipeline.bufferMask]
		skip, err := c.pipeline.ApplyProcessors(event.msg)
		if skip || err != nil {
			event.err = err
			event.skip = skip
		}
	}
}

type SendConsumer struct {
	pipeline *DisruptorPipeline
}

func (c SendConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		event := c.pipeline.ringBuffer[lower&c.pipeline.bufferMask]
		if event.skip || event.err != nil {
			c.pipeline.GetInput().Ack(event.msg, event.err)
			continue
		}
		w := &sync.WaitGroup{}
		w.Add(1)
		event.msg.SetMeta(core.AckWaitGroup, w)
		c.pipeline.GetOutput().Process(event.msg)
	}
}

type AckConsumer struct {
	pipeline *DisruptorPipeline
}

func (c AckConsumer) Consume(lower, upper int64) {
	for ; lower <= upper; lower++ {
		event := c.pipeline.ringBuffer[lower&c.pipeline.bufferMask]
		if event.skip || event.err != nil {
			c.pipeline.outQueueCount++
			// should already been acked in previous consumer
			continue
		}
		w := c.getWaitGroup(event.msg)
		// this should never happen
		if w == nil {
			c.pipeline.GetLogger().Fatal("AckWaitGroup missing", log.String("msg id", event.msg.Header.ID),
				log.Any("data", event.msg.Data))
			continue
		}
		w.Wait()
		c.pipeline.outQueueCount++
	}
}

func (c AckConsumer) getWaitGroup(msg *core.Message) *sync.WaitGroup {
	o, ok := msg.GetMeta(core.AckWaitGroup)
	if !ok {
		return nil
	}
	w, ok := o.(*sync.WaitGroup)
	if !ok {
		return nil
	}
	return w
}

func (p *DisruptorPipeline) StartPipeline() (err error) {
	// start processors first
	for i := len(p.GetProcessors()) - 1; i >= 0; i-- {
		each := p.GetProcessors()[i]
		if err = each.Start(); err != nil {
			return
		}
	}
	// start disruptor
	p.ringBuffer = make([]*DisruptorEvent, p.config.BufferSize)
	processGroup := make([]disruptor.Consumer, 0)
	for i := 0; i < p.config.Concurrency; i++ {
		processGroup = append(processGroup, ProcessConsumer{
			num:             int64(i),
			concurrencyMask: int64(p.config.Concurrency - 1),
			pipeline:        p,
		})
	}
	d := disruptor.New(
		disruptor.WithCapacity(int64(p.config.BufferSize)),
		disruptor.WithWriteBlockParkTime(100*time.Nanosecond),
		disruptor.WithConsumerGroup(
			processGroup...,
		),
		disruptor.WithConsumerGroup(
			SendConsumer{
				pipeline: p,
			},
		),
		disruptor.WithConsumerGroup(
			AckConsumer{
				pipeline: p,
			},
		),
	)
	p.disruptor = d
	go p.disruptor.Read()
	go p.checkStatus()
	return
}

func (p *DisruptorPipeline) Ack(msg *core.Message, err error) {
	if wg, ok := msg.GetMeta(core.AckWaitGroup); ok {
		wg.(*sync.WaitGroup).Done()
	}
	p.GetInput().Ack(msg, err)
}

func (p *DisruptorPipeline) Process(msg *core.Message) {
	sequence := p.disruptor.Reserve(1)
	p.ringBuffer[sequence&p.bufferMask] = &DisruptorEvent{msg: msg}
	p.disruptor.Commit(sequence, sequence)
	p.inQueueCount++
}

func (p *DisruptorPipeline) StopPipeline() {
	p.stop = true
	if err := p.disruptor.Close(); err != nil {
		p.GetLogger().Error("stop error", log.String("id", p.GetID()), log.Error(err))
	}
}

func (p *DisruptorPipeline) Configure(config core.StringMap) (err error) {
	if err = p.BasePipeline.Configure(config); err != nil {
		return
	}

	c := &DisruptorPipelineConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	p.config = c
	if p.config.Concurrency == 0 {
		p.config.Concurrency = DefaultDisruptorConcurrency
	}
	if p.config.BufferSize == 0 {
		p.config.BufferSize = DefaultDisruptorBufferSize
	}
	p.bufferMask = int64(p.config.BufferSize) - 1
	return
}

// checkStatus check status of pipeline at a constant interval, if it finds stuck will close the pipeline
func (p *DisruptorPipeline) checkStatus() {
	ticker := time.NewTicker(DefaultCheckProgressInterval)
	defer ticker.Stop()
	prevOut := p.outQueueCount
	for {
		select {
		case <-ticker.C:
			if p.stop {
				return
			}

			p.GetLogger().Info("in pipe msg", log.String("id", p.GetID()), log.Uint64("count", p.inQueueCount-p.outQueueCount))
			if prevOut != 0 && p.outQueueCount-prevOut == 0 && p.inQueueCount-p.outQueueCount != 0 {
				p.GetLogger().Error("pipe stuck, exiting checkStatus", log.String("id", p.GetID()))
				p.RaiseError(fmt.Errorf("pipe stuck"))
				return
			}
		}
	}
}

package pipeline

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type BasePipeline struct {
	*core.BaseInput
	input            core.Input
	processors       []core.Processor
	LifeCycleFunctor BasePipelineLifeCycle
}

type BasePipelineLifeCycle interface {
	StartPipeline() error
	StopPipeline()
}

func NewBasePipeline() *BasePipeline {
	return &BasePipeline{
		BaseInput:  core.NewBaseInput(),
		processors: make([]core.Processor, 0),
	}
}

func (p *BasePipeline) Configure(config core.StringMap) (err error) {
	if err = p.BaseComponent.Configure(config); err != nil {
		return
	}
	processorConfigs, err := utils.GetConfigArrayFromConfig(config, "$.Processors")
	if err != nil {
		return
	}
	for _, each := range processorConfigs {
		if processor, err := core.GetComponentBuilderInstance().CreateProcessor(each); err != nil {
			return err
		} else {
			p.processors = append(p.processors, processor)
		}
	}
	return
}

func (p *BasePipeline) SetInput(input core.Input) {
	p.input = input
}

func (p *BasePipeline) GetInput() core.Input {
	return p.input
}

func (p *BasePipeline) SetProcessors(processors []core.Processor) {
	p.processors = processors
}

func (p *BasePipeline) GetProcessors() []core.Processor {
	return p.processors
}

func (p *BasePipeline) ApplyProcessors(msg *core.Message) (skip bool, err error) {
	for _, processor := range p.processors {
		skip, err = processor.Process(msg)
		if err != nil {
			p.GetLogger().Error("process message error", log.String("id", processor.GetID()), log.Error(err))
			p.GetInput().Ack(msg, err)
			return
		}
		if skip {
			p.GetInput().Ack(msg, err)
			return
		}
	}
	return
}

func (p *BasePipeline) Errors() chan error {
	return p.input.Errors()
}

func (p *BasePipeline) Start() (err error) {
	if err = p.GetOutput().Start(); err != nil {
		return
	}
	if err = p.LifeCycleFunctor.StartPipeline(); err != nil {
		return
	}
	if err = p.GetInput().Start(); err != nil {
		return
	}
	return
}

func (p *BasePipeline) Stop() {
	p.GetInput().Stop()
	p.LifeCycleFunctor.StopPipeline()
	p.GetOutput().Stop()
}

func (p *BasePipeline) SetState(state []byte) (err error) {
	err = p.GetInput().SetState(state)
	return
}

func (p *BasePipeline) GetState() ([]byte, bool) {
	return p.GetInput().GetState()
}

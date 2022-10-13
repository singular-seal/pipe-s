package builder

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/input/mysql/binlog"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/output/dummy"
	"github.com/singular-seal/pipe-s/pkg/output/logoutput"
	"github.com/singular-seal/pipe-s/pkg/pipeline/simple"
	"github.com/singular-seal/pipe-s/pkg/processor/converter"
	"github.com/singular-seal/pipe-s/pkg/processor/json"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

// DefaultComponentBuilder is a Component factory.
type DefaultComponentBuilder struct {
	constructorMap map[string]core.ComponentConstructor
	logger         *log.Logger
}

func NewDefaultComponentBuilder() *DefaultComponentBuilder {
	return &DefaultComponentBuilder{constructorMap: make(map[string]core.ComponentConstructor)}
}

func (b *DefaultComponentBuilder) SetLogger(logger *log.Logger) {
	b.logger = logger
}

// RegisterComponent register a component type. After registering, ComponentBuilder is able to create corresponding
// component.
func (b *DefaultComponentBuilder) RegisterComponent(typeName string, constructor core.ComponentConstructor) {
	b.constructorMap[typeName] = constructor
}

// createComponent creates a component by config.
func (b *DefaultComponentBuilder) createComponent(config core.StringMap) (core.Component, error) {
	t, err := utils.GetStringFromConfig(config, "$.Type")
	if err != nil {
		return nil, err
	}
	constructor, ok := b.constructorMap[t]
	if !ok {
		return nil, fmt.Errorf("constructor not found:%s", t)
	}
	comp := constructor()
	err = comp.Configure(config)
	if la, ok := comp.(core.LogAware); ok {
		la.SetLogger(b.logger)
	}
	return comp, err
}

func (b *DefaultComponentBuilder) createInput(config core.StringMap, parent core.Output) (input core.Input, err error) {
	c, err := b.createComponent(config)
	if err != nil {
		return
	}
	input, ok := c.(core.Input)
	if !ok {
		err = fmt.Errorf("not input:%s", c.GetID())
		return
	}
	input.SetOutput(parent)
	// input maybe pipeline
	pipe, ok := input.(core.Pipeline)
	if !ok {
		return
	}

	ic, err := utils.GetConfigFromConfig(config, "$.Input")
	if err != nil || ic == nil {
		b.logger.Info("pipeline has not input", log.String("ID", pipe.GetID()))
		return
	}
	iip, err := b.createInput(ic, pipe)
	if err != nil {
		return
	}
	pipe.SetInput(iip)
	return
}

func (b *DefaultComponentBuilder) createOutput(parent core.Input, config core.StringMap) (output core.Output, err error) {
	c, err := b.createComponent(config)
	if err != nil {
		return
	}
	output, ok := c.(core.Output)
	if !ok {
		err = fmt.Errorf("not output:%s", c.GetID())
		return
	}
	output.SetInput(parent)

	pipe, ok := output.(core.Pipeline)
	if !ok {
		return
	}

	oc, err := utils.GetConfigFromConfig(config, "$.Output")
	if err != nil || oc == nil {
		b.logger.Info("pipeline has not output", log.String("ID", pipe.GetID()))
		return
	}
	oop, err := b.createOutput(pipe, oc)
	if err != nil {
		return
	}
	pipe.SetOutput(oop)
	return
}

func (b *DefaultComponentBuilder) CreateProcessor(config core.StringMap) (processor core.Processor, err error) {
	if comp, err := b.createComponent(config); err != nil {
		return nil, err
	} else {
		if p, ok := comp.(core.Processor); !ok {
			return nil, fmt.Errorf("not processor:%s", comp.GetID())
		} else {
			return p, nil
		}
	}
}

// CreatePipeline creates a pipeline by config.
func (b *DefaultComponentBuilder) CreatePipeline(config core.StringMap) (pipe core.Pipeline, err error) {
	c, err := b.createComponent(config)
	if err != nil {
		return nil, err
	}
	pipe = c.(core.Pipeline)

	ic, err := utils.GetConfigFromConfig(config, "$.Input")
	if err != nil || ic == nil {
		b.logger.Info("pipeline has not input", log.String("ID", pipe.GetID()))
	} else {
		input, err := b.createInput(ic, pipe)
		if err != nil {
			return nil, err
		}
		pipe.SetInput(input)
	}
	oc, err := utils.GetConfigFromConfig(config, "$.Output")
	if err != nil || oc == nil {
		b.logger.Info("pipeline has not output", log.String("ID", pipe.GetID()))
	} else {
		output, err := b.createOutput(pipe, oc)
		if err != nil {
			return nil, err
		}
		pipe.SetOutput(output)
	}
	return
}

// InitComponentBuilder registers predefined components.
func InitComponentBuilder(logger *log.Logger) {

	dc := NewDefaultComponentBuilder()
	dc.SetLogger(logger)
	core.SetComponentBuilderInstance(dc)

	dc.RegisterComponent("MysqlBinlogInput", func() core.Component {
		return binlog.NewMysqlBinlogInput()
	})
	dc.RegisterComponent("DisruptorPipeline", func() core.Component {
		return simple.NewDisruptorPipeline()
	})
	dc.RegisterComponent("LogOutput", func() core.Component {
		return logoutput.NewLogOutput()
	})
	dc.RegisterComponent("DummyOutput", func() core.Component {
		return dummy.NewDummyOutput()
	})

	dc.RegisterComponent("JsonMarshaller", func() core.Component {
		return json.NewJsonMarshaller()
	})
	dc.RegisterComponent("MysqlDMLToDBChangeConverter", func() core.Component {
		return converter.NewMysqlDMLToDBChangeConverter()
	})

}

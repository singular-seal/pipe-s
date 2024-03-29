package builder

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	kafka2 "github.com/singular-seal/pipe-s/pkg/input/kafka"
	"github.com/singular-seal/pipe-s/pkg/input/mysql/binlog"
	"github.com/singular-seal/pipe-s/pkg/input/mysql/scan"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/output/debug"
	"github.com/singular-seal/pipe-s/pkg/output/dummy"
	"github.com/singular-seal/pipe-s/pkg/output/kafka"
	"github.com/singular-seal/pipe-s/pkg/output/logoutput"
	"github.com/singular-seal/pipe-s/pkg/output/mysql/batch"
	"github.com/singular-seal/pipe-s/pkg/output/mysql/check"
	"github.com/singular-seal/pipe-s/pkg/output/mysql/stream"
	"github.com/singular-seal/pipe-s/pkg/pipeline/simple"
	"github.com/singular-seal/pipe-s/pkg/processor/converter"
	"github.com/singular-seal/pipe-s/pkg/processor/filter"
	"github.com/singular-seal/pipe-s/pkg/processor/json"
	"github.com/singular-seal/pipe-s/pkg/processor/mapping"
	"github.com/singular-seal/pipe-s/pkg/processor/value"
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
		return nil, errors.Errorf("constructor not found:%s", t)
	}
	comp := constructor()
	err = comp.Configure(config)
	if la, ok := comp.(core.LogAware); ok {
		la.SetLogger(b.logger)
	}
	return comp, err
}

func (b *DefaultComponentBuilder) createInput(parent core.Output, config core.StringMap) (input core.Input, err error) {
	c, err := b.createComponent(config)
	if err != nil {
		return
	}
	input, ok := c.(core.Input)
	if !ok {
		err = errors.Errorf("not input:%s", c.GetID())
		return
	}
	input.SetOutput(parent)
	input.SetErrors(parent.Errors())
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
	iip, err := b.createInput(pipe, ic)
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
		err = errors.Errorf("not output:%s", c.GetID())
		return
	}
	output.SetInput(parent)
	output.SetErrors(parent.Errors())

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
			return nil, errors.Errorf("not processor:%s", comp.GetID())
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
		input, err := b.createInput(pipe, ic)
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
	dc.RegisterComponent("MysqlScanInput", func() core.Component {
		return scan.NewMysqlScanInput()
	})
	dc.RegisterComponent("KafkaInput", func() core.Component {
		return kafka2.NewKafkaInput()
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
	dc.RegisterComponent("DebugOutput", func() core.Component {
		return debug.NewDebugOutput()
	})
	dc.RegisterComponent("MysqlBatchOutput", func() core.Component {
		return batch.NewMysqlBatchOutput()
	})
	dc.RegisterComponent("MysqlCheckOutput", func() core.Component {
		return check.NewMysqlCheckOutput()
	})
	dc.RegisterComponent("MysqlStreamOutput", func() core.Component {
		return stream.NewMysqlStreamOutput()
	})
	dc.RegisterComponent("KafkaOutput", func() core.Component {
		return kafka.NewKafkaOutput()
	})

	dc.RegisterComponent("JsonMarshaller", func() core.Component {
		return json.NewJsonMarshaller()
	})
	dc.RegisterComponent("DBChangeUnmarshaller", func() core.Component {
		return json.NewDBChangeUnmarshaller()
	})
	dc.RegisterComponent("MysqlDMLToDBChangeConverter", func() core.Component {
		return converter.NewMysqlDMLToDBChangeConverter()
	})
	dc.RegisterComponent("DBChangeMappingProcessor", func() core.Component {
		return mapping.NewDBChangeMappingProcessor()
	})
	dc.RegisterComponent("MysqlDMLFilter", func() core.Component {
		return filter.NewMysqlDMLFilter()
	})
	dc.RegisterComponent("DBTableNameCatcher", func() core.Component {
		return value.NewDBTableNameCatcher()
	})
	dc.RegisterComponent("ColumnValueProcessor", func() core.Component {
		return value.NewColumnValueProcessor()
	})
	dc.RegisterComponent("ValueMapper", func() core.Component {
		return value.NewValueMapper()
	})
}

package value

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type ColumnValueConfig struct {
	TableNameVariable   string
	OutputVariable      string
	TableColumnMappings map[string]string
}

type ColumnValueProcessor struct {
	*core.BaseComponent
	config *ColumnValueConfig
}

func NewColumnValueProcessor() *ColumnValueProcessor {
	return &ColumnValueProcessor{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (p *ColumnValueProcessor) Configure(config core.StringMap) (err error) {
	c := &ColumnValueConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	p.config = c
	if len(p.config.TableNameVariable) == 0 || len(p.config.OutputVariable) == 0 || len(p.config.TableColumnMappings) == 0 {
		return fmt.Errorf("config missing")
	}
	return nil
}

func (p *ColumnValueProcessor) Process(msg *core.Message) (bool, error) {
	event := msg.Data.(*core.DBChangeEvent)
	tb, ok := msg.GetVariable(p.config.TableNameVariable)
	if !ok {
		return false, fmt.Errorf("no table variable, msg id %s, db %s, table %s", msg.Header.ID,
			event.Database, event.Table)
	}
	msg.SetVariable(p.config.OutputVariable, event.GetRow()[tb.(string)])
	return false, nil
}

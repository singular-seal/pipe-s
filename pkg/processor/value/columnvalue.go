package value

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type ColumnValueConfig struct {
	TableNameVariable   string            // where to get the table name
	OutputVariable      string            // the output variable name to put the column value
	TableColumnMappings map[string]string // the column name to get value for each table
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
		return errors.New("config missing")
	}
	return nil
}

func (p *ColumnValueProcessor) Process(msg *core.Message) (bool, error) {
	event := msg.Data.(*core.DBChangeEvent)
	tb, ok := msg.GetVariable(p.config.TableNameVariable)
	if !ok {
		return false, errors.Errorf("no table variable, msg id %s, db %s, table %s", msg.Header.ID,
			event.Database, event.Table)
	}
	col, ok := p.config.TableColumnMappings[tb.(string)]
	if !ok {
		return false, errors.Errorf("column mapping not found, msg id %s, db %s, table %s", msg.Header.ID,
			event.Database, event.Table)
	}
	msg.SetVariable(p.config.OutputVariable, event.GetRow()[col])
	return false, nil
}

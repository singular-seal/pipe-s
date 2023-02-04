package value

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type ValueMapperConfig struct {
	InputVariable  string
	OutputVariable string
	Mappings       map[string]string
}

type ValueMapper struct {
	*core.BaseComponent
	config *ValueMapperConfig
}

func NewValueMapper() *ValueMapper {
	return &ValueMapper{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (m *ValueMapper) Configure(config core.StringMap) (err error) {
	c := &ValueMapperConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	m.config = c
	if len(m.config.InputVariable) == 0 || len(m.config.OutputVariable) == 0 || len(m.config.Mappings) == 0 {
		return fmt.Errorf("config missing")
	}
	return nil
}

func (m *ValueMapper) Process(msg *core.Message) (bool, error) {
	event := msg.Data.(*core.DBChangeEvent)
	v, ok := msg.GetVariable(m.config.InputVariable)
	if !ok {
		return false, fmt.Errorf("no input variable, msg id %s, db %s, table %s", msg.Header.ID,
			event.Database, event.Table)
	}
	msg.SetVariable(m.config.OutputVariable, event.GetRow()[m.config.Mappings[v.(string)]])
	return false, nil
}

package json

import (
	"encoding/json"
	"github.com/singular-seal/pipe-s/pkg/core"
)

type JsonMarshaller struct {
	*core.BaseComponent
}

func NewJsonMarshaller() *JsonMarshaller {
	return &JsonMarshaller{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (m *JsonMarshaller) Process(msg *core.Message) (skip bool, err error) {
	if data, err := json.Marshal(msg.Body); err == nil {
		msg.Body = data
		msg.Type = core.TypeJsonByte
	}
	return
}

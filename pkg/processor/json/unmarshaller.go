package json

import (
	"encoding/json"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
)

type DBChangeUnmarshaller struct {
	*core.BaseComponent
}

func NewDBChangeUnmarshaller() *DBChangeUnmarshaller {
	return &DBChangeUnmarshaller{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (um *DBChangeUnmarshaller) Process(msg *core.Message) (skip bool, err error) {
	var event core.DBChangeEvent
	d, ok := msg.Data.([]byte)
	if !ok {
		return false, fmt.Errorf("")
	}
	if err = json.Unmarshal(d, &event); err != nil {
		return
	}
	msg.Type = core.TypeDBChange
	msg.Data = &event
	return
}

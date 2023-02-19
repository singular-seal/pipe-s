package json

import (
	"encoding/json"
	"github.com/pkg/errors"
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
	d, ok := msg.Body.([]byte)
	if !ok {
		return false, errors.Errorf("no byte found, msg id %s", msg.Header.ID)
	}
	if err = json.Unmarshal(d, &event); err != nil {
		return
	}
	msg.Type = core.TypeDBChange
	msg.Body = &event
	return
}

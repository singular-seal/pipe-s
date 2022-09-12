package debug

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
)

type DebugOutput struct {
	*core.BaseOutput
}

func NewDebugOutput() *DebugOutput {
	return &DebugOutput{
		BaseOutput: core.NewBaseOutput(),
	}
}

func (out *DebugOutput) Process(msg *core.Message) {
	event := msg.Data.(*core.DBChangeEvent)
	id := event.GetRow()["id"]
	if id.(int32) == 13908 {
		fmt.Println(fmt.Sprintf("id:%s op:%s", msg.Header.ID, event.Operation))
	}
	out.GetInput().Ack(msg, nil)
}

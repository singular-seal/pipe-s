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
	event := msg.Body.(*core.DBChangeEvent)
	id := event.GetRow()["id"]
	if id.(int32) == 2963 {
		fmt.Println(fmt.Sprintf("id:%s op:%s", msg.Header.ID, event.Operation))
	}
	out.GetInput().Ack(msg, nil)
}

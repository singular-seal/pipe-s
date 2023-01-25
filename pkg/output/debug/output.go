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
	if id == 77 {
		fmt.Println("found")
	}
	out.GetInput().Ack(msg, nil)
}

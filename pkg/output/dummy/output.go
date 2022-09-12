package dummy

import "github.com/singular-seal/pipe-s/pkg/core"

// DummyOutput output messages to nothing, just for testing.
type DummyOutput struct {
	*core.BaseOutput
}

func NewDummyOutput() *DummyOutput {
	return &DummyOutput{
		BaseOutput: core.NewBaseOutput(),
	}
}

func (out *DummyOutput) Process(msg *core.Message) {
	out.GetInput().Ack(msg, nil)
}

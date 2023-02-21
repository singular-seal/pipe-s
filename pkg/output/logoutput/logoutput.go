package logoutput

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
)

// LogOutput output messages to logger, usually for testing.
type LogOutput struct {
	*core.BaseOutput
}

func NewLogOutput() *LogOutput {
	return &LogOutput{
		BaseOutput: core.NewBaseOutput(),
	}
}

func (out *LogOutput) Process(msg *core.Message) {
	if msg.Type == core.TypeJsonBytes {
		out.GetLogger().Info("msg", log.String("id", msg.Header.ID), log.String("data", string(msg.Data.([]byte))))
	} else {
		out.GetLogger().Info("msg", log.String("id", msg.Header.ID))
	}
	out.GetInput().Ack(msg, nil)
}

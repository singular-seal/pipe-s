package simple

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type TestOutput struct {
	*core.BaseOutput
	Count int64
}

func (out *TestOutput) Process(msg *core.Message) {
	atomic.AddInt64(&out.Count, 1)
	out.GetInput().Ack(msg, nil)
}

type TestInput struct {
	*core.BaseInput
}

func (in *TestInput) SetState(state []byte) error {
	return nil
}

func (in *TestInput) GetState() (state []byte, done bool) {
	return nil, false
}

func (in *TestInput) Ack(msg *core.Message, err error) {
}

func (in *TestInput) Start() (err error) {
	ms := make([]*core.Message, 0)
	for i := 0; i < 10000; i++ {
		m := &core.Message{
			Header: &core.MessageHeader{
				ID:      fmt.Sprintf("%d", i),
				MetaMap: map[int]interface{}{},
			},
		}
		ms = append(ms, m)
	}
	for _, m := range ms {
		in.GetOutput().Process(m)
	}
	return
}

func TestDisruptorPipeline_Process(t *testing.T) {
	r := require.New(t)
	p := NewDisruptorPipeline()
	p.Configure(core.StringMap{
		"Processors": []interface{}{},
	})
	in := &TestInput{
		BaseInput: core.NewBaseInput(),
	}
	p.SetInput(in)
	in.SetOutput(p)

	o := &TestOutput{
		BaseOutput: core.NewBaseOutput(),
	}
	p.SetOutput(o)
	o.SetInput(p)

	p.Start()

	time.Sleep(time.Second)
	r.Equal(o.Count, int64(10000))
}

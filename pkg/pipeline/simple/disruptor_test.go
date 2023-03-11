package simple

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

type TestOutput struct {
	*core.BaseOutput
	Count    int
	Current  int
	Disorder bool
}

func (out *TestOutput) Process(msg *core.Message) {
	out.Count++
	id, _ := strconv.Atoi(msg.Header.ID)
	if id < out.Current {
		out.Disorder = true
	}
	out.Current = id
	out.GetInput().Ack(msg, nil)
}

type TestInput struct {
	*core.BaseInput
	Current  int
	Disorder bool
}

func (in *TestInput) SetState(state []byte) error {
	return nil
}

func (in *TestInput) GetState() (state []byte, done bool) {
	return nil, false
}

func (in *TestInput) Ack(msg *core.Message, err error) {
	id, _ := strconv.Atoi(msg.Header.ID)
	if id < in.Current {
		in.Disorder = true
	}
	in.Current = id
}

func (in *TestInput) Start() (err error) {
	ms := make([]*core.Message, 0)
	for i := 0; i < 1000000; i++ {
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
		Current:   -1,
	}
	p.SetInput(in)
	in.SetOutput(p)

	o := &TestOutput{
		BaseOutput: core.NewBaseOutput(),
		Current:    -1,
	}
	p.SetOutput(o)
	o.SetInput(p)

	p.Start()

	time.Sleep(time.Second)
	r.False(in.Disorder)
	r.False(o.Disorder)
	r.Equal(o.Count, 1000000)
}

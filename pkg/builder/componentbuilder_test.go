package builder

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefaultComponentBuilder_CreatePipeline(t *testing.T) {
	InitComponentBuilder(log.Default())
	conf := core.StringMap{
		"Type": "DisruptorPipeline",
		"Input": core.StringMap{
			"Type": "KafkaInput",
		},
		"Output": core.StringMap{
			"Type": "DummyOutput",
		},
		"Processors": []interface{}{
			core.StringMap{
				"Type": "MysqlDMLToDBChangeConverter",
			},
			core.StringMap{
				"Type": "JsonMarshaller",
			},
		},
	}
	p, err := core.GetComponentBuilderInstance().CreatePipeline(conf)
	r := require.New(t)
	r.Nil(err)
	_, ok := p.(core.Pipeline)
	r.True(ok)

	ic := conf["Input"].(core.StringMap)
	ic["Type"] = "NonExistInput"
	_, err = core.GetComponentBuilderInstance().CreatePipeline(conf)
	r.NotNil(err, "Should not create unregistered input.")

	ic["Type"] = "KafkaInput"
	delete(conf, "Processors")
	_, err = core.GetComponentBuilderInstance().CreatePipeline(conf)
	r.NotNil(err, "Should not create without Processors config.")
}

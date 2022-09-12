package value

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDBTableNameCatcher_Process(t *testing.T) {
	r := require.New(t)
	conf := core.StringMap{
		"TableNamePattern":  "^([a-z]+)\\d+$",
		"TableNameVariable": "logicalTable",
		"DBNamePattern":     "^([a-z]+)\\d+$",
		"DBNameVariable":    "logicalDB",
	}
	p := NewDBTableNameCatcher()
	p.Configure(conf)

	e := &core.MysqlDMLEvent{
		FullTableName: "srcdb1.srctab1",
		Operation:     core.DBInsert,
	}

	m := &core.Message{
		Type: core.TypeDML,
		Header: &core.MessageHeader{
			MetaMap: map[int]interface{}{},
		},
		Data: e,
	}
	p.Process(m)
	v, _ := m.GetVariable("logicalTable")
	r.Equal(v, "srctab")
	v, _ = m.GetVariable("logicalDB")
	r.Equal(v, "srcdb")

	delete(conf, "TableNamePattern")
	delete(conf, "DBNamePattern")
	p = NewDBTableNameCatcher()
	p.Configure(conf)
	p.Process(m)
	v, _ = m.GetVariable("logicalTable")
	r.Equal(v, "srctab1")
	v, _ = m.GetVariable("logicalDB")
	r.Equal(v, "srcdb1")
}

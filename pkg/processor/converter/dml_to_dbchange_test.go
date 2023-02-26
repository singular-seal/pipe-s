package converter

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMysqlDMLToDBChangeConverter_Process(t *testing.T) {
	msg := &core.Message{
		Type: core.TypeDML,
		Header: &core.MessageHeader{
			MetaMap: map[int]interface{}{
				core.MetaTableSchema: &core.Table{
					TableName: "test_tab",
					DBName:    "test_db",
					Columns: []*core.Column{
						{
							Name:  "id",
							Index: 0,
						},
						{
							Name:  "name",
							Index: 1,
						},
					},
				},
			},
			ID: "test_msg",
		},
		Data: &core.MysqlDMLEvent{
			FullTableName: "test_db.test_tab",
			Operation:     core.DBUpdate,
			OldRow:        []interface{}{101, "Dog"},
			NewRow:        []interface{}{101, "Wolf"},
		},
	}
	p := NewMysqlDMLToDBChangeConverter()
	p.Configure(core.StringMap{})
	p.Process(msg)

	r := require.New(t)
	r.Equal(msg.Type, core.TypeDBChange)
	e, ok := msg.Data.(*core.DBChangeEvent)
	r.True(ok)
	r.Equal(e.Database, "test_db")
	r.Equal(e.Table, "test_tab")
	r.Equal(e.OldRow["name"], "Dog")
	r.Equal(e.NewRow["name"], "Wolf")
}

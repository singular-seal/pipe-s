package mapping

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDBChangeMappingProcessor_Process(t *testing.T) {
	r := require.New(t)
	p := NewDBChangeMappingProcessor()
	conf := core.StringMap{
		"DBNameVariable":    "logicalDB",
		"TableNameVariable": "logicalTable",
		"Mappings": []interface{}{
			core.StringMap{
				"Source": core.StringMap{
					"DB": "src_db",
					"Tables": []interface{}{
						core.StringMap{
							"Table": "src_foo_tab",
							"Columns": []interface{}{
								"id", "src_name",
							},
							"Actions": []interface{}{
								"insert", "update",
							},
						},
						core.StringMap{
							"Table": "src_bar_tab",
						},
					},
				},
				"Target": core.StringMap{
					"DB": "dest_db",
					"Tables": []interface{}{
						core.StringMap{
							"Table": "dest_foo_tab",
							"Columns": []interface{}{
								"id", "dest_name",
							},
							"Actions": []interface{}{
								"update", "delete",
							},
						},
						core.StringMap{
							"Table": "dest_bar_tab",
						},
					},
				},
			},
		},
	}
	p.Configure(conf)
	e := &core.DBChangeEvent{
		Database:  "src_db1",
		Table:     "src_foo_tab1",
		Operation: core.DBInsert,
		NewRow: map[string]interface{}{
			"id":       1,
			"src_name": "event1",
		},
	}
	m := &core.Message{
		Type: core.TypeDBChange,
		Header: &core.MessageHeader{
			MetaMap: map[int]interface{}{
				core.CustomVariable: map[string]interface{}{
					"logicalDB":    "src_db",
					"logicalTable": "src_foo_tab",
				},
			},
		},
		Data: e,
	}
	p.Process(m)
	r.Equal(e.Database, "dest_db1")
	r.Equal(e.Table, "dest_foo_tab1")
	r.Equal(e.Operation, core.DBUpdate)
	r.Equal(e.OldRow["id"], 1)
	r.Equal(e.NewRow["dest_name"], "event1")

	delete(conf, "DBNameVariable")
	delete(conf, "TableNameVariable")
	p = NewDBChangeMappingProcessor()
	p.Configure(conf)

	e = &core.DBChangeEvent{
		Database:  "src_db",
		Table:     "src_bar_tab",
		Operation: core.DBDelete,
		OldRow: map[string]interface{}{
			"id":   2,
			"name": "event2",
		},
	}
	m = &core.Message{
		Type: core.TypeDBChange,
		Header: &core.MessageHeader{
			MetaMap: map[int]interface{}{},
		},
		Data: e,
	}
	p.Process(m)
	r.Equal(e.Database, "dest_db")
	r.Equal(e.Table, "dest_bar_tab")
	r.Equal(e.Operation, core.DBDelete)
	r.Equal(e.OldRow["id"], 2)
	r.Equal(e.OldRow["name"], "event2")

}

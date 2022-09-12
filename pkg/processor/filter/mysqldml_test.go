package filter

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMysqlDMLFilter_Process(t *testing.T) {
	r := require.New(t)
	f := NewMysqlDMLFilter()
	f.Configure(core.StringMap{
		"FullTableNamePattern": "^test_db.*\\.test_tab\\d+$",
		"Operations":           []string{"insert", "update"},
	})

	m := &core.Message{
		Data: &core.MysqlDMLEvent{
			FullTableName: "test_db10.test_tab12",
			Operation:     core.DBInsert,
		},
	}
	ok, _ := f.Process(m)
	r.False(ok)
	m = &core.Message{
		Data: &core.MysqlDMLEvent{
			FullTableName: "test_db10.test_tab",
			Operation:     core.DBUpdate,
		},
	}
	ok, _ = f.Process(m)
	r.True(ok)
	m = &core.Message{
		Data: &core.MysqlDMLEvent{
			FullTableName: "test_db10.test_tab16",
			Operation:     core.DBDelete,
		},
	}
	ok, _ = f.Process(m)
	r.True(ok)
}

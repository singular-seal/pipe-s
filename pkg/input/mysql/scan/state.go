package scan

import (
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"sync/atomic"
)

type TableState struct {
	TableName         string
	DBName            string
	ColumnStatesValue atomic.Value  `json:"-"`
	ColumnStates      []interface{} // for json marshall and unmarshall
	EstimatedCount    int64
	FinishedCount     int64
	Done              bool
}

func InitTableState(state *TableState, table *core.Table, conn *sql.DB) (err error) {
	state.TableName = table.TableName
	state.DBName = table.DBName
	state.FinishedCount = 0
	state.Done = false

	if state.EstimatedCount, err = getRowCount(table.DBName, table.TableName, conn); err != nil {
		return
	}
	if state.EstimatedCount == 0 {
		state.Done = true
	}
	return
}

func getRowCount(dbName string, tableName string, conn *sql.DB) (int64, error) {
	statement := fmt.Sprintf("SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbName, tableName)
	var count sql.NullInt64
	row := conn.QueryRow(statement)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	if !count.Valid {
		return 0, errors.Errorf("table_rows_invalid")
	}
	return count.Int64, nil
}

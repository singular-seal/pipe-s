package scan

import (
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"sync/atomic"
)

type TableState struct {
	TableName      string
	DBName         string
	ColumnStates   atomic.Value
	EstimatedCount int64
	FinishedCount  int64
	Done           bool
}

func InitTableState(state *TableState, table *core.Table, conn *sql.DB) (err error) {
	state.TableName = table.TableName
	state.DBName = table.DBName
	state.FinishedCount = 0
	state.Done = false

	if state.EstimatedCount, err = GetRowCount(table.DBName, table.TableName, conn); err != nil {
		return
	}
	if state.EstimatedCount == 0 {
		state.Done = true
	}
	return
}

func GetRowCount(dbName string, tableName string, conn *sql.DB) (int64, error) {
	statement := fmt.Sprintf("select table_rows from information_schema.tables where table_schema = '%s' and table_name = '%s'", dbName, tableName)
	var count sql.NullInt64
	row := conn.QueryRow(statement)
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	if !count.Valid {
		return 0, fmt.Errorf("table_rows_invalid")
	}
	return count.Int64, nil
}

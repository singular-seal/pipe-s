package utils

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/singular-seal/pipe-s/pkg/core"
	"math/rand"
	"reflect"
	"strings"
	"time"
)

const (
	DefaultMysqlConnectionTimeout = 10000 // milliseconds
	DefaultMysqlReadTimeout       = 10000
	DefaultMysqlWriteTimeout      = 10000
)

// CreateMysqlClient creates a new instance of mysql connection
func CreateMysqlClient(host string, port uint16, user string, password string) (db1 *sql.DB, err error) {
	dbDSNTimeouts := "&timeout=%vms&readTimeout=%vms&writeTimeout=%vms"
	dbDSNTimeouts = fmt.Sprintf(dbDSNTimeouts, DefaultMysqlConnectionTimeout, DefaultMysqlReadTimeout, DefaultMysqlWriteTimeout)

	dbDSN := "%s:%s@tcp(%s:%d)/?interpolateParams=true&parseTime=true&multiStatements=true&collation=utf8mb4_general_ci%s"
	dbDSN = fmt.Sprintf(dbDSN, user, password, host, port, dbDSNTimeouts)

	if db1, err = sql.Open("mysql", dbDSN); err != nil {
		return nil, err
	}

	timeoutDuration := time.Duration(DefaultMysqlReadTimeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	if err = db1.PingContext(ctx); err != nil {
		return nil, err
	}

	return db1, nil
}

// CloseMysqlClient closes mysql client connection
func CloseMysqlClient(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return db.Close()
}

func GenerateRandomServerID() uint32 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	for {
		id := r.Uint32() % 10000000
		if id > 0 {
			return id
		}
	}
}

type DDLInfo struct {
	DB    string
	Table string
	Node  ast.StmtNode
}

// ExtractFromDDL extracts DDL information from statement.
func ExtractFromDDL(schema []byte, stmt ast.StmtNode) []*DDLInfo {
	result := make([]*DDLInfo, 0)
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.Name,
				Table: "",
				Node:  stmt,
			})

	case *ast.DropDatabaseStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.Name,
				Table: "",
				Node:  stmt,
			})

	case *ast.CreateTableStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.Table.Schema.String(),
				Table: v.Table.Name.String(),
				Node:  stmt,
			})

	case *ast.DropTableStmt:
		for i := range v.Tables {
			dropTableStmt := *v
			dropTableStmt.Tables = nil
			dropTableStmt.Tables = append(dropTableStmt.Tables, v.Tables[i])
			result = append(result,
				&DDLInfo{
					DB:    v.Tables[i].Schema.String(),
					Table: v.Tables[i].Name.String(),
					Node:  &dropTableStmt,
				})
		}

	case *ast.AlterTableStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.Table.Schema.String(),
				Table: v.Table.Name.String(),
				Node:  stmt,
			})

	case *ast.TruncateTableStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.Table.Schema.String(),
				Table: v.Table.Name.String(),
				Node:  stmt,
			})

	case *ast.RenameTableStmt:
		result = append(result,
			&DDLInfo{
				DB:    v.OldTable.Schema.String(),
				Table: v.OldTable.Name.String(),
				Node:  stmt,
			})

	default:
		result = append(result,
			&DDLInfo{
				DB:    "",
				Table: "",
				Node:  stmt,
			})
	}

	if len(result) == 1 && result[0].DB == "" {
		result[0].DB = string(schema)
	}
	return result
}

// QuoteColumns add quotes to column names
func QuoteColumns(cols []string) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = fmt.Sprintf("`%s`", col)
	}
	return result
}

func genKeyValueSqlAndArgs(columns map[string]interface{}, separator string) (string, []interface{}) {
	args := make([]interface{}, 0)
	conditions := make([]string, 0)
	for k, v := range columns {
		args = append(args, v)
		conditions = append(conditions, fmt.Sprintf("`%s`=?", k))
	}
	return strings.Join(conditions, separator), args
}

func genKeyValueSqlAndArgsExclude(columns map[string]interface{}, exclude map[string]interface{},
	separator string) (string, []interface{}) {
	args := make([]interface{}, 0)
	conditions := make([]string, 0)
	for k, v := range columns {
		if _, ok := exclude[k]; ok {
			continue
		}
		args = append(args, v)
		conditions = append(conditions, fmt.Sprintf("`%s`=?", k))
	}
	return strings.Join(conditions, separator), args
}

func genColumnsStringAndArgs(columns map[string]interface{}) (string, string, []interface{}) {
	args := make([]interface{}, 0)
	cols := make([]string, 0)
	marks := make([]string, 0)
	for k, v := range columns {
		args = append(args, v)
		cols = append(cols, k)
		marks = append(marks, "?")
	}
	return strings.Join(QuoteColumns(cols), ","), strings.Join(marks, ","), args
}

func getKeys(eventData map[string]interface{}, keyColumns []string) map[string]interface{} {
	result := make(map[string]interface{})
	for _, column := range keyColumns {
		result[column] = eventData[column]
	}
	return result
}

func GenerateSqlAndArgs(event *core.DBChangeEvent, keyColumns []string) (sqlString string, sqlArgs []interface{}) {
	switch event.Operation {
	case core.DBInsert:
		colString, markString, args := genColumnsStringAndArgs(event.NewRow)
		sqlString = fmt.Sprintf("insert ignore into %s.%s (%s) values (%s)", event.Database,
			event.Table, colString, markString)
		sqlArgs = args
	case core.DBUpdate:
		keys := getKeys(event.NewRow, keyColumns)
		setString, setArgs := genKeyValueSqlAndArgsExclude(event.NewRow, keys, ",")
		condString, condArgs := genKeyValueSqlAndArgs(keys, " and ")
		sqlString = fmt.Sprintf("update %s.%s set %s where %s", event.Database, event.Table, setString, condString)
		sqlArgs = append(setArgs, condArgs...)
	case core.DBDelete:
		keys := getKeys(event.OldRow, keyColumns)
		condString, args := genKeyValueSqlAndArgs(keys, " and ")
		sqlString = fmt.Sprintf("delete from %s.%s where %s", event.Database, event.Table, condString)
		sqlArgs = args
	default:
	}
	return
}

func NewBatchDataPointers(columnTypes []*sql.ColumnType, size int) [][]interface{} {
	ret := make([][]interface{}, size)
	for idx := 0; idx < size; idx++ {
		vPtrs := make([]interface{}, len(columnTypes))
		for columnIdx := range columnTypes {
			scanType := GetScanType(columnTypes[columnIdx])
			vptr := reflect.New(scanType)
			vPtrs[columnIdx] = vptr.Interface()
		}
		ret[idx] = vPtrs
	}
	return ret
}

func ScanRowsWithDataPointers(rows *sql.Rows, columnTypes []*sql.ColumnType, vPtrs []interface{}) ([]interface{}, error) {
	if err := rows.Scan(vPtrs...); err != nil {
		return nil, errors.Trace(err)
	}

	for i := range columnTypes {
		p, err := getScanPtrSafe(i, columnTypes, vPtrs)
		if err != nil {
			return nil, err
		}
		vPtrs[i] = p
	}
	return vPtrs, nil
}

func getScanPtrSafe(columnIdx int, columnTypes []*sql.ColumnType, vPtrs []interface{}) (interface{}, error) {
	scanType := GetScanType(columnTypes[columnIdx])
	if scanType.String() == "sql.RawBytes" {
		data := reflect.ValueOf(vPtrs[columnIdx]).Elem().Interface()
		dataRawBytes, ok := data.(sql.RawBytes)
		if !ok {
			return nil, fmt.Errorf("failed_convert_sql.RawBytes")
		}
		var b sql.RawBytes
		if dataRawBytes != nil {
			b = make(sql.RawBytes, len(dataRawBytes))
			copy(b, dataRawBytes)
		}
		return &b, nil
	}

	return vPtrs[columnIdx], nil
}

func GetScanType(columnType *sql.ColumnType) reflect.Type {
	if isFloatColumn(columnType) {
		return reflect.TypeOf(sql.NullFloat64{})
	} else if isStringColumn(columnType) {
		return reflect.TypeOf(sql.NullString{})
	} else {
		return columnType.ScanType()
	}
}

func isStringColumn(columnType *sql.ColumnType) bool {
	typeName := columnType.DatabaseTypeName()
	return strings.Contains(typeName, "TEXT") ||
		strings.Contains(typeName, "CHAR") ||
		strings.Contains(typeName, "JSON")
}

func isFloatColumn(columnType *sql.ColumnType) bool {
	typeName := columnType.DatabaseTypeName()
	return strings.Contains(typeName, "DECIMAL")
}

func GetColumnTypes(db string, table string, cols []string, conn *sql.DB) ([]*sql.ColumnType, error) {
	var colStat string
	if len(cols) == 0 {
		colStat = "*"
	} else {
		colStat = strings.Join(cols, ",")
	}

	stat := fmt.Sprintf("select %s from `%s`.`%s` limit 1", colStat, db, table)
	rows, err := conn.Query(stat)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func FullTableName(db string, table string) string {
	return db + "." + table
}

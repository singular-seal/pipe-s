package utils

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser/ast"
	"math/rand"
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

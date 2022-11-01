package schema

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

const TableNotFoundError = 1146

// SimpleSchemaStore always get the newest table schemas from db.
// DDL event will trigger refreshing.
type SimpleSchemaStore struct {
	schemas map[string]map[string]*core.Table // dbname->tablename->*FullTableName
	conn    *sql.DB
	ownConn bool
	logger  *log.Logger
}

func NewSimpleSchemaStoreWithParameters(host string, port uint16, user string, password string) (*SimpleSchemaStore, error) {
	InitGlobal()
	client, err := utils.CreateMysqlClient(host, port, user, password)
	if err != nil {
		return nil, err
	}
	return &SimpleSchemaStore{
		schemas: make(map[string]map[string]*core.Table),
		conn:    client,
		ownConn: true,
	}, nil
}

func NewSimpleSchemaStoreWithClient(client *sql.DB) *SimpleSchemaStore {
	InitGlobal()
	return &SimpleSchemaStore{
		schemas: make(map[string]map[string]*core.Table),
		conn:    client,
		ownConn: false,
	}
}

func (s *SimpleSchemaStore) InitWithClient(client *sql.DB) {
	InitGlobal()
	s.conn = client
}

func (s *SimpleSchemaStore) SetLogger(logger *log.Logger) {
	s.logger = logger
}

func (s *SimpleSchemaStore) GetType() string {
	return simple
}

func (s *SimpleSchemaStore) Close() {
	if !s.ownConn {
		return
	}
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			s.logger.Error("close schema store error", log.Error(err))
		}
	}
}

// GetTable first tries to return table schema from cache,
// if not found then query from DB.
func (s *SimpleSchemaStore) GetTable(db string, table string) (ts *core.Table, err error) {
	tables, ok := s.schemas[db]
	if !ok {
		tables = make(map[string]*core.Table)
		s.schemas[db] = tables
	}
	if ts, ok := tables[table]; ok {
		return ts, nil
	}
	if ts, err = s.readFromDB(db, table); err != nil {
		return
	}
	if ts == nil {
		return
	}
	tables[table] = ts
	return
}

// DeleteTable remove table schema from cache
func (s *SimpleSchemaStore) DeleteTable(db string, table string) error {
	tables, ok := s.schemas[db]
	if !ok {
		return nil
	}
	if table == "" {
		delete(s.schemas, db)
	} else {
		delete(tables, table)
	}
	return nil
}

// readFromDB fetches table schema from database
func (s *SimpleSchemaStore) readFromDB(db string, table string) (ts *core.Table, err error) {
	stmt := fmt.Sprintf("show columns from %s.%s", db, table)
	rows, err := s.conn.Query(stmt)
	if err != nil {
		sqlErr, ok := err.(*mysql.MySQLError)
		if !ok {
			return nil, err
		}
		// This can happen when table is removed from db ,we can't treat it as an error.
		if sqlErr.Number == TableNotFoundError {
			s.logger.Info("table not found", log.String("db", db), log.String("table", table))
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	ts = &core.Table{
		TableName: table,
		DBName:    db,
		Columns:   make([]*core.Column, 0),
		PKColumns: make([]*core.Column, 0),
	}

	var columnName string
	var rawType string
	var columnKey string
	var index = 0
	var nullable string
	var defaultValue sql.NullString
	var placeHolder sql.NullString

	for rows.Next() {
		if err = rows.Scan(&columnName, &rawType, &nullable, &columnKey, &defaultValue, &placeHolder); err != nil {
			return nil, err
		}
		var column = core.Column{
			Name:  columnName,
			Index: index,
		}

		index++
		if columnKey == "PRI" {
			column.IsPrimaryKey = true
			ts.PKColumns = append(ts.PKColumns, &column)
		} else {
			column.IsPrimaryKey = false
		}
		if nullable == "NO" {
			column.IsNullable = false
		} else {
			column.IsNullable = true
		}
		column.DefaultValue = defaultValue
		column.RawType = rawType
		fillTypeInfo(&column)
		ts.Columns = append(ts.Columns, &column)
	}

	return ts, nil
}

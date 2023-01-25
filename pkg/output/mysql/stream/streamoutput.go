package stream

import (
	"database/sql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/schema"
)

type MysqlStreamOutput struct {
	*core.BaseOutput
	conn        *sql.DB
	schemaStore schema.SchemaStore // the schema store to load table schemas

}

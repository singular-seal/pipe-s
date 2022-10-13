package schema

import (
	"github.com/singular-seal/pipe-s/pkg/core"
)

type SchemaStore interface {
	core.LogAware
	GetTable(db string, table string) (*core.Table, error)
	DeleteTable(db string, table string) error
	GetType() string
	Close()
}

const (
	simple = "simple"
)

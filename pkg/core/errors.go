package core

import (
	"github.com/pkg/errors"
)

func TypeError(value interface{}, typeName string) error {
	return errors.Errorf("%v is not %s", value, typeName)
}

func NoSchemaError(db string, table string) error {
	return errors.Errorf("no table schema:%s.%s\"", db, table)
}

func NoPKError(db string, table string) error {
	return errors.Errorf("no primary key:%s.%s\"", db, table)
}

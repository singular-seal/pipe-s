package core

import "fmt"

func TypeError(value interface{}, typeName string) error {
	return fmt.Errorf("%v is not %s", value, typeName)
}

func NoSchemaError(db string, table string) error {
	return fmt.Errorf("no table schema:%s.%s\"", db, table)
}

func NoPKError(db string, table string) error {
	return fmt.Errorf("no primary key:%s.%s\"", db, table)
}

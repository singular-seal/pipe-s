package core

import (
	"database/sql"
)

// Column represents a mysql table column
type Column struct {
	Name  string
	Index int
	Type  ColumnType
	// Description from mysql metadata
	RawType      string
	IsPrimaryKey bool
	IsNullable   bool
	IsUnsigned   bool
	//some types may have length property
	Length       int
	DefaultValue sql.NullString
}

type ColumnType = int

// mysql column types, see https://dev.mysql.com/doc/refman/8.0/en/data-types.html
const (
	TypeOther ColumnType = iota + 1
	TypeTinyInt
	TypeSmallInt
	TypeMediumInt
	TypeInt
	TypeBigInt
	TypeDecimal
	TypeFloat
	TypeDouble
	TypeBit
	TypeDate
	TypeDatetime
	TypeTimestamp
	TypeTime
	TypeYear
	TypeChar
	TypeVarchar
	TypeBinary
	TypeVarBinary
	TypeTinyBlob
	TypeBlob
	TypeMediumBlob
	TypeLongBlob
	TypeTinyText
	TypeText
	TypeMediumText
	TypeLongText
	TypeEnum
	TypeSet
	TypeJson
)

// Table represents a mysql table
type Table struct {
	TableName string
	DBName    string
	Columns   []*Column
	PKColumns []*Column
}

func (t *Table) ColumnNames() []string {
	result := make([]string, 0)
	for _, each := range t.Columns {
		result = append(result, each.Name)
	}
	return result
}

func (t *Table) PKColumnNames() []string {
	result := make([]string, 0)
	for _, each := range t.PKColumns {
		result = append(result, each.Name)
	}
	return result
}

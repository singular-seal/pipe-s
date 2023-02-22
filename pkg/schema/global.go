package schema

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var ColumnNameTypeMapping map[string]core.ColumnType

// regex for strings like 'bigint unsigned' varchar(255) which are returned from 'show columns from ...'
var columnTypeRegex = regexp.MustCompile(`^(?P<type>\w+)(\((?P<arg1>.+?)(,.+)*\))?`)

var initOnce sync.Once

func InitGlobal() {
	initOnce.Do(func() {
		ColumnNameTypeMapping = initColumnTypeMapping()
	})
}

func initColumnTypeMapping() map[string]core.ColumnType {
	result := make(map[string]core.ColumnType)
	result["tinyint"] = core.TypeTinyInt
	result["smallint"] = core.TypeSmallInt
	result["mediumint"] = core.TypeMediumInt
	result["int"] = core.TypeInt
	result["bigint"] = core.TypeBigInt
	result["decimal"] = core.TypeDecimal
	result["float"] = core.TypeFloat
	result["double"] = core.TypeDouble
	result["bit"] = core.TypeBit
	result["date"] = core.TypeDate
	result["datetime"] = core.TypeDatetime
	result["timestamp"] = core.TypeTimestamp
	result["time"] = core.TypeTime
	result["year"] = core.TypeYear
	result["char"] = core.TypeChar
	result["varchar"] = core.TypeVarchar
	result["binary"] = core.TypeBinary
	result["varbinary"] = core.TypeVarBinary
	result["tinyblob"] = core.TypeTinyBlob
	result["blob"] = core.TypeBlob
	result["mediumblob"] = core.TypeMediumBlob
	result["longblob"] = core.TypeLongBlob
	result["tinytext"] = core.TypeTinyText
	result["text"] = core.TypeText
	result["mediumtext"] = core.TypeMediumText
	result["longtext"] = core.TypeLongText
	result["enum"] = core.TypeEnum
	result["set"] = core.TypeSet
	result["json"] = core.TypeJson
	return result
}

func getWidth(arg string) int {
	if len(arg) == 0 {
		return 0
	}
	width, err := strconv.Atoi(arg)
	if err != nil {
		return 0
	}
	return width
}

// fillTypeInfo fills column schema with type info
func fillTypeInfo(column *core.Column) {
	rawType := column.RawType
	matches := columnTypeRegex.FindStringSubmatch(rawType)
	typeString := matches[1]
	arg1 := matches[3]
	unsigned := strings.Contains(strings.ToLower(rawType), "unsigned")

	switch typeString {
	case "date", "enum", "set", "json":
		column.Type = ColumnNameTypeMapping[typeString]
	case "tinyint", "smallint", "mediumint", "int", "bigint", "float", "double":
		column.Type = ColumnNameTypeMapping[typeString]
		column.IsUnsigned = unsigned
	case "bit", "datetime", "timestamp", "time", "year", "char", "varchar", "binary", "varbinary", "blob", "text",
		"tinyblob", "mediumblob", "longblob", "tinytext", "mediumtext", "longtext":
		column.Type = ColumnNameTypeMapping[typeString]
		column.Length = getWidth(arg1)
	case "decimal":
		column.Type = ColumnNameTypeMapping[typeString]
		column.Length = getWidth(arg1)
		column.IsUnsigned = unsigned
	default:
		column.Type = core.TypeOther
	}
}

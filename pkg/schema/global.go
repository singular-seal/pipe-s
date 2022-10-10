package schema

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var ColumnNameTypeMapping map[string]ColumnType

// pattern for strings like 'bigint unsigned' varchar(255) which are returned from 'show columns from ...'
var columnTypePattern = regexp.MustCompile(`^(?P<type>\w+)(\((?P<arg1>.+?)(,.+)*\))?`)

var initOnce sync.Once

func InitGlobal() {
	initOnce.Do(func() {
		ColumnNameTypeMapping = initColumnTypeMapping()
	})
}

func initColumnTypeMapping() map[string]ColumnType {
	result := make(map[string]ColumnType)
	result["tinyint"] = TypeTinyInt
	result["smallint"] = TypeSmallInt
	result["mediumint"] = TypeMediumInt
	result["int"] = TypeInt
	result["bigint"] = TypeBigInt
	result["decimal"] = TypeDecimal
	result["float"] = TypeFloat
	result["double"] = TypeDouble
	result["bit"] = TypeBit
	result["date"] = TypeDate
	result["datetime"] = TypeDatetime
	result["timestamp"] = TypeTimestamp
	result["time"] = TypeTime
	result["year"] = TypeYear
	result["char"] = TypeChar
	result["varchar"] = TypeVarchar
	result["binary"] = TypeBinary
	result["varbinary"] = TypeVarBinary
	result["tinyblob"] = TypeTinyBlob
	result["blob"] = TypeBlob
	result["mediumblob"] = TypeMediumBlob
	result["longblob"] = TypeLongBlob
	result["tinytext"] = TypeTinyText
	result["text"] = TypeText
	result["mediumtext"] = TypeMediumText
	result["longtext"] = TypeLongText
	result["enum"] = TypeEnum
	result["set"] = TypeSet
	result["json"] = TypeJson
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
func fillTypeInfo(column *Column) {
	rawType := column.RawType
	matches := columnTypePattern.FindStringSubmatch(rawType)
	typeString := matches[1]
	arg1 := matches[3]
	unsigned := strings.Contains(strings.ToLower(rawType), "unsigned")

	switch typeString {
	case "decimal":
		column.Type = ColumnNameTypeMapping[typeString]
		column.Length = getWidth(arg1)
		column.IsUnsigned = unsigned
	case "tinyint", "smallint", "mediumint", "int", "bigint", "float", "double":
		column.Type = ColumnNameTypeMapping[typeString]
		column.IsUnsigned = unsigned
	case "bit", "datetime", "timestamp", "time", "year", "char", "varchar", "binary", "varbinary", "blob", "text",
		"tinyblob", "mediumblob", "longblob", "tinytext", "mediumtext", "longtext":
		column.Type = ColumnNameTypeMapping[typeString]
		column.Length = getWidth(arg1)
	case "date", "enum", "set", "json":
		column.Type = ColumnNameTypeMapping[typeString]
	default:
		column.Type = TypeOther
	}
}

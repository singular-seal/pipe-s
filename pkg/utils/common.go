package utils

import (
	"hash/fnv"
	"reflect"
	"time"
)

// GetTypeName returns type's name as string
func GetTypeName(v interface{}) string {
	return reflect.TypeOf(v).String()
}

func ReadDataFromPointers(pointers []interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, p := range pointers {
		result = append(result, reflect.ValueOf(p).Elem().Interface())
	}
	return result
}

func IntervalCheckTicker(intervalMS int64) *time.Ticker {
	return time.NewTicker(time.Millisecond * time.Duration(intervalMS/10+1))
}

func GetFNV64aHash(text string) int {
	algorithm := fnv.New64a()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum64())
}

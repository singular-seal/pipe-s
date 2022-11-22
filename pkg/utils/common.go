package utils

import "reflect"

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

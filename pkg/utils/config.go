package utils

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/yalp/jsonpath"
	"strconv"
)

func GetStringFromConfig(config core.StringMap, path string) (string, error) {
	obj, err := jsonpath.Read(config, path)
	if err != nil {
		return "", err
	}
	value, ok := obj.(string)
	if !ok {
		return "", core.TypeError(obj, "string")
	}
	return value, err
}

func GetConfigArrayFromConfig(config core.StringMap, path string) ([]core.StringMap, error) {
	obj, err := jsonpath.Read(config, path)
	if err != nil {
		return nil, err
	}
	oa, ok := obj.([]interface{})
	if !ok {
		return nil, core.TypeError(obj, "Array")
	}
	result := make([]core.StringMap, 0)
	for _, each := range oa {
		cm, ok := each.(core.StringMap)
		if !ok {
			return nil, core.TypeError(obj, "StringMap")
		}
		result = append(result, cm)
	}
	return result, nil
}

func GetConfigFromConfig(config core.StringMap, path string) (core.StringMap, error) {
	obj, err := jsonpath.Read(config, path)
	if err != nil {
		return nil, err
	}
	value, ok := obj.(core.StringMap)
	if !ok {
		return nil, core.TypeError(obj, "StringMap")
	}
	return value, err
}

func GetIntFromConfig(config core.StringMap, path string) (int, error) {
	obj, err := jsonpath.Read(config, path)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(fmt.Sprint(obj))
	if err != nil {
		return 0, core.TypeError(obj, "int")
	}
	return value, err
}

// ConfigToStruct converts a map to corresponding struct.
func ConfigToStruct(source core.StringMap, target interface{}) error {
	return mapstructure.Decode(source, target)
}

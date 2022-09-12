package core

import "fmt"

func TypeError(value interface{}, typeName string) error {
	return fmt.Errorf("%v is not %s", value, typeName)
}

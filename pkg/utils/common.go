package utils

import (
	"context"
	"hash/fnv"
	"net"
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

// IntervalCheckTicker ensure things happen in intervalMS period so use a much smaller check interval
// than intervalMS to prevent the extreme near 2*intervalMS situation.
func IntervalCheckTicker(intervalMS int64) *time.Ticker {
	return time.NewTicker(time.Millisecond * time.Duration(intervalMS/10+1))
}

func GetFNV64aHash(text string) int {
	algorithm := fnv.New64a()
	algorithm.Write([]byte(text))
	return int(algorithm.Sum64())
}

func AbsInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func LookupDNS(host string) ([]string, error) {
	const DNSTimeout = 5000
	timeoutDuration := time.Duration(DNSTimeout) * time.Millisecond
	ctx, _ := context.WithTimeout(context.Background(), timeoutDuration)

	return net.DefaultResolver.LookupHost(ctx, host)
}

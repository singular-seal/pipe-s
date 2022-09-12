package utils

import (
	"context"
	"hash/fnv"
	"net"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
)

// GetTypeName returns type's name as string
func GetTypeName(v interface{}) string {
	return reflect.TypeOf(v).String()
}

func ReadDataFromPointers(pointers []interface{}) []interface{} {
	result := make([]interface{}, 0, len(pointers))
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

func SignalQuit() chan os.Signal {
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return quit
}

// IsNil works around the famous issue https://stackoverflow.com/questions/13476349/check-for-nil-and-nil-interface-in-go
func IsNil(v interface{}) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}

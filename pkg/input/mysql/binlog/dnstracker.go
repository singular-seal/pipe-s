package binlog

import (
	"context"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"reflect"
	"sort"
	"time"
)

const DefaultDNSCheckInterval = 5000 // milliseconds

type DNSTracker struct {
	host            string
	ipList          []string
	changeCallback  func()
	stopWaitContext context.Context
	stopCancel      context.CancelFunc
	logger          *log.Logger
}

func NewDNSTracker(host string, changeCallback func(), logger *log.Logger) *DNSTracker {
	return &DNSTracker{
		host:           host,
		ipList:         make([]string, 0),
		changeCallback: changeCallback,
		logger:         logger,
	}
}

func (t *DNSTracker) Start() {
	t.stopWaitContext, t.stopCancel = context.WithCancel(context.Background())
	t.logger.Info("DNSTracker starting")

	ipList, err := t.resolveHost()
	if err != nil {
		t.logger.Error("failed to resolve dns", log.Error(err))
	} else {
		t.ipList = ipList
	}

	go t.run()
}

func (t *DNSTracker) run() {
	ticker := time.NewTicker(time.Duration(DefaultDNSCheckInterval) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopWaitContext.Done():
			return
		case <-ticker.C:
			if t.detectDNSChange() {
				t.changeCallback()
			}
		}
	}
}

func (t *DNSTracker) Stop() {
	t.stopCancel()
}

func (t *DNSTracker) resolveHost() (ipList []string, err error) {
	ipList, err = utils.LookupDNS(t.host)
	if err != nil {
		return nil, err
	}

	if len(ipList) == 0 {
		err = fmt.Errorf("empty ip list for host:%s", t.host)
		return
	}

	sort.Strings(ipList)
	size := 1
	// remove duplicates
	for i := 1; i < len(ipList); i++ {
		if ipList[i] != ipList[i-1] {
			ipList[size] = ipList[i]
			size++
		}
	}
	ipList = ipList[:size]
	return
}

func (t *DNSTracker) detectDNSChange() (changed bool) {
	ipList, err := t.resolveHost()
	if err != nil {
		t.logger.Error("failed to resolve dns", log.Error(err))
		return
	}
	// represents one detecting failed
	if len(t.ipList) == 0 {
		t.ipList = ipList
		return
	}

	if reflect.DeepEqual(t.ipList, ipList) {
		return
	}
	changed = true
	return
}

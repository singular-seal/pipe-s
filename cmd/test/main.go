package main

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
)

type MyHandler struct {
}

func (h *MyHandler) Handle(e *replication.BinlogEvent) error {
	if e.Header.LogPos%65536 == 0 {
		fmt.Println(e.Header.LogPos)
	}
	return nil
}

func (h *MyHandler) HandleError(err error) {
}

func (h *MyHandler) Close() {
}

func main() {
	/*	config := replication.DisruptorBinlogSyncerConfig{
			BinlogSyncerConfig: replication.BinlogSyncerConfig{
				ServerID:        1000,
				Flavor:          "mysql",
				Host:            "192.168.18.14",
				Port:            3306,
				User:            "admin",
				Password:        "admin",
				Localhost:       "11111",
				ReadTimeout:     60 * time.Second,
				HeartbeatPeriod: 1 * time.Second,
				RecvBufferSize:  1024 * 1024,
				UseDecimal:      true,
			},
			BufferSize:       8192,
			ParseConcurrency: 4,
			//SaveGTIDInterval: 100,
		}
		syncer := replication.NewDisruptorBinlogSyncer(config, &MyHandler{})
		set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", "e7e77cbc-16f8-11ed-a055-c4bde5be8098", 1, 10))
		syncer.StartSyncGTID(set)

		time.Sleep(time.Second * 3600)
	*/

}

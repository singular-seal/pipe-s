package scan

import (
	"context"
	"database/sql"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"sync"
)

const (
	DefaultBatchSize   = 128
	DefaultConcurrency = 8
)

type MysqlScanInputConfig struct {
	Host           string
	Port           uint16
	User           string
	Password       string
	TableNameRegex string // select tables need to be replicated by regex
	BatchSize      int
	Concurrency    int
}

type MysqlScanInput struct {
	*core.BaseInput
	Config        *MysqlScanInputConfig
	dbConnection  *sql.DB
	schemaStore   schema.SchemaStore
	scanState     *sync.Map
	tableScanners []*TableScanner // table scanners in this stream
	ctx           context.Context
	cancelFunc    context.CancelFunc
	errorsChan    chan error
	lastError     error
}

type ScanTask struct {
	TableName string
	DBName    string
	Schema    *core.Table
}

type TableScanner struct {
	id            int
	input         *MysqlScanInput
	BatchSize     int
	PendingTables chan *ScanTask
	// last message of each table processed
	LastMessages  *sync.Map
	HasError      bool
	MaxUpdateTime int64
}

package check

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
	"time"
)

const (
	DefaultFlushIntervalMS = 100
	DefaultFlushBatchSize  = 500
)

type MysqlCheckOutputConfig struct {
	ID       string
	Host     string
	Port     uint16
	User     string
	Password string

	OutputFilePath string
	// if we need skip checking for most recently updated rows, we can use UpdateTimeColumn and UpdateTimeSkipSeconds
	// to construct a query condition like 'UpdateTimeColumn<Now()-UpdateTimeSkipSeconds'
	UpdateTimeColumn      string
	UpdateTimeSkipSeconds int64
	TableBufferSize       int   // max messages buffered for each table
	TableFlushIntervalMS  int64 // max ms between each table buffer flushing
}

type MysqlCheckOutput struct {
	*core.BaseOutput
	config          *MysqlCheckOutputConfig
	tableProcessors map[string]*TableProcessor
	conn            *sql.DB
	schemaStore     schema.SchemaStore // the schema store to load table schemas

	stopWaitContext context.Context
	stopCancel      context.CancelFunc
}

func NewMysqlCheckOutput() *MysqlCheckOutput {
	ctx, cancelFunc := context.WithCancel(context.Background())

	output := &MysqlCheckOutput{
		BaseOutput:      core.NewBaseOutput(),
		tableProcessors: make(map[string]*TableProcessor),
		stopWaitContext: ctx,
		stopCancel:      cancelFunc,
	}
	return output
}

func (o *MysqlCheckOutput) Configure(config core.StringMap) (err error) {
	c := &MysqlCheckOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c

	if o.config.TableFlushIntervalMS == 0 {
		o.config.TableFlushIntervalMS = DefaultFlushIntervalMS
	}

	if o.config.TableBufferSize == 0 {
		o.config.TableBufferSize = DefaultFlushBatchSize
	}
	return nil
}

type TableProcessor struct {
	messages      chan *core.Message
	output        *MysqlCheckOutput
	table         string
	flushSig      chan bool
	lastFlushTime int64
	conn          *sql.DB
	stopContext   context.Context
	logger        *log.Logger
}

func NewTableProcessor(table string, output *MysqlCheckOutput) *TableProcessor {
	proc := &TableProcessor{
		output:      output,
		table:       table,
		messages:    make(chan *core.Message, output.config.TableBufferSize),
		flushSig:    make(chan bool),
		conn:        output.conn,
		stopContext: output.stopWaitContext,
		logger:      output.GetLogger(),
	}
	return proc
}

func (p *TableProcessor) Run() {
	p.lastFlushTime = time.Now().UnixNano() / 1e6

	go func() {
		ticker := utils.IntervalCheckTicker(p.output.config.TableFlushIntervalMS)
		for {
			select {
			case <-p.stopContext.Done():
				p.logger.Info("processor exit", log.String("table", p.table))
				return
			case <-ticker.C:
				if time.Now().UnixNano()/1e6-p.lastFlushTime > p.output.config.TableFlushIntervalMS {
					p.Flush()
				}
			case <-p.flushSig:
				p.Flush()
			}
		}
	}()
}

func (p *TableProcessor) Flush() {
	p.lastFlushTime = time.Now().UnixNano() / 1e6
	size := len(p.messages)
	if size == 0 {
		return
	}

	messages := make([]*core.Message, 0)
	for i := 0; i < size; i++ {
		messages = append(messages, <-p.messages)
	}

	err := p.check(messages)
	for _, message := range messages {
		p.output.GetInput().Ack(message, err)
	}
}

func (p *TableProcessor) check(messages []*core.Message) error {
	return nil
}

func generateSqlAndArgs(db string, table string, selCols []string, pkCols []string, pkValues [][]interface{}) (string, []interface{}) {
	sqlPrefix := fmt.Sprintf("select %s from %s.%s where (%s) in", strings.Join(selCols, ","), db, table,
		strings.Join(pkCols, ","))

	batchPlaceHolders := make([]string, 0)
	batchArgs := make([]interface{}, 0)
	for _, pkValue := range pkValues {
		phs := make([]string, 0)
		for _, v := range pkValue {
			phs = append(phs, "?")
			batchArgs = append(batchArgs, v)
		}
		batchPlaceHolders = append(batchPlaceHolders, fmt.Sprintf("%s", strings.Join(phs, ",")))
	}
	s := []string{sqlPrefix, fmt.Sprintf("(%s)", strings.Join(batchPlaceHolders, ","))}
	return strings.Join(s, " "), batchArgs
}

func getPKValues(pkCols []string, messages []*core.Message) [][]interface{} {
	result := make([][]interface{}, 0)
	for _, msg := range messages {
		event := msg.Data.(*core.DBChangeEvent)
		pk := make([]interface{}, 0)
		for _, col := range pkCols {
			pk = append(pk, event.GetRow()[col])
		}
		result = append(result, pk)
	}
	return result
}

func (p *TableProcessor) Process(m *core.Message) {
	p.messages <- m
	if len(p.messages) >= p.output.config.TableBufferSize {
		p.flushSig <- true
	}
}

func (o *MysqlCheckOutput) Process(m *core.Message) {
	dbChange := m.Data.(*core.DBChangeEvent)
	ft := utils.FullTableName(dbChange.Database, dbChange.Table)
	p, ok := o.tableProcessors[ft]

	if !ok {
		p = NewTableProcessor(ft, o)
		p.Run()
	}
	p.Process(m)
}

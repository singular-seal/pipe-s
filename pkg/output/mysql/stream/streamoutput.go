package stream

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"sync"
)

const (
	DefaultConcurrency     = 4
	DefaultTaskQueueLength = 1000
)

type MysqlStreamOutput struct {
	*core.BaseOutput
	config *MysqlStreamOutputConfig
	conn   *sql.DB

	taskQueues      []chan *core.Message
	stopWaitContext context.Context
	stopCancel      context.CancelFunc
	stopWait        *sync.WaitGroup
}

type MysqlStreamOutputConfig struct {
	ID              string
	Host            string
	Port            uint16
	User            string
	Password        string
	Concurrency     int
	TaskQueueLength int
}

func NewMysqlStreamOutput() *MysqlStreamOutput {
	output := &MysqlStreamOutput{
		BaseOutput: core.NewBaseOutput(),
	}
	return output
}

func (o *MysqlStreamOutput) Start() (err error) {
	if o.conn, err = utils.CreateMysqlClient(o.config.Host, o.config.Port, o.config.User, o.config.Password); err != nil {
		return
	}
	o.conn.SetMaxIdleConns(o.config.Concurrency)
	o.stopWaitContext, o.stopCancel = context.WithCancel(context.Background())
	o.stopWait = &sync.WaitGroup{}

	o.taskQueues = make([]chan *core.Message, o.config.Concurrency)
	for i := 0; i < o.config.Concurrency; i++ {
		o.taskQueues[i] = make(chan *core.Message, o.config.TaskQueueLength)
		o.stopWait.Add(1)
		go o.processTaskQueue(i)
	}

	return nil
}

func (o *MysqlStreamOutput) Stop() {
	o.stopCancel()
	o.stopWait.Wait()

	if err := utils.CloseMysqlClient(o.conn); err != nil {
		o.GetLogger().Error("MysqlBatchOutput failed close db connection")
	}
}

func (o *MysqlStreamOutput) Configure(config core.StringMap) (err error) {
	c := &MysqlStreamOutputConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	o.config = c
	if o.config.Concurrency == 0 {
		o.config.Concurrency = DefaultConcurrency
	}
	if o.config.TaskQueueLength == 0 {
		o.config.TaskQueueLength = DefaultTaskQueueLength
	}
	return nil
}

func (o *MysqlStreamOutput) processTaskQueue(index int) {
	for {
		select {
		case <-o.stopWaitContext.Done():
			o.stopWait.Done()
			return
		case event := <-o.taskQueues[index]:
			o.processTask(event)
		}
	}
}

func (o *MysqlStreamOutput) processTask(msg *core.Message) {
	event := msg.Data.(*core.SQLEvent)
	result, err := o.conn.Exec(event.SQLString, event.SQLArgs...)
	if err != nil {
		o.GetInput().Ack(msg, err)
	} else {
		if c, err1 := result.RowsAffected(); err1 != nil || c == 0 {
			o.GetLogger().Warn("no row affected", log.String("sql", event.SQLString),
				log.String("id", msg.Header.ID), log.Error(err1))
		}
		o.GetInput().Ack(msg, nil)
	}
}

func (o *MysqlStreamOutput) Process(m *core.Message) {
	rk, ok := m.GetMeta(core.RoutingKey)
	if !ok {
		o.GetInput().Ack(m, fmt.Errorf("no routing key:%s", m.Header.ID))
		return
	}
	o.taskQueues[rk.(int)%o.config.Concurrency] <- m
}

package task

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/builder"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/metrics"
	"github.com/singular-seal/pipe-s/pkg/statestore"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"sync"
	"time"
)

const (
	// StateKey is default key in state store
	StateKey                   = "state"
	DefaultSyncStateIntervalMS = 10000
)

// Task is the runnable instance to do all ETL tasks.
type Task interface {
	// GetID returns the id of the task.
	GetID() string

	// Start starts the task and blocks until task finished or error thrown.
	Start() error

	// Stop stops the task and blocks until stopped.
	Stop()
}

type DefaultTask struct {
	ID                  string
	config              core.StringMap
	syncStateIntervalMS int

	stateStore core.StateStore
	pipeline   core.Pipeline
	stop       chan struct{} // signal task needs to stop
	stopped    chan struct{} // signal that task has stopped
	stopOnce   sync.Once

	lastError error // the last error in the executed pipeline
	logger    *log.Logger
}

func NewTask(config core.StringMap) Task {
	return &DefaultTask{
		config:  config,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func NewTaskFromJson(path string) (Task, error) {
	configData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "fail read file")
	}
	confObj := make(core.StringMap)
	if err = json.Unmarshal(configData, &confObj); err != nil {
		return nil, errors.Wrap(err, "fail unmarshal json")
	}
	taskObj, err := utils.GetConfigFromConfig(confObj, "$.Task")
	if err != nil {
		return nil, errors.Wrap(err, "fail read config")
	}
	return NewTask(taskObj), nil
}

func (t *DefaultTask) configure() (err error) {
	if t.ID, err = utils.GetStringFromConfig(t.config, "$.ID"); err != nil {
		return err
	}
	storeConfig, err := utils.GetConfigFromConfig(t.config, "$.StateStore")
	if err != nil {
		return
	}
	if t.stateStore, err = statestore.CreateStateStore(storeConfig); err != nil {
		return
	}
	syncInterval, err := utils.GetIntFromConfig(storeConfig, "$.SyncIntervalMS")

	if err == nil && syncInterval != 0 {
		t.syncStateIntervalMS = syncInterval
	} else {
		t.syncStateIntervalMS = DefaultSyncStateIntervalMS
	}

	pipeConfig, err := utils.GetConfigFromConfig(t.config, "$.Pipeline")
	if err != nil {
		return
	}
	if t.pipeline, err = core.GetComponentBuilderInstance().CreatePipeline(pipeConfig); err != nil {
		return
	}

	return
}

func (t *DefaultTask) startPprof() {
	port, err := utils.GetIntFromConfig(t.config, "$.PProfPort")
	if err != nil || port == 0 {
		return
	}
	go func() {
		t.logger.Info("starting pprof", log.Int("port", port))
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	}()
}

// syncState load state from pipeline and save to state store periodically.
func (t *DefaultTask) syncState() {
	ticker := time.NewTicker(time.Duration(t.syncStateIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-t.pipeline.Errors():
			// try save state before stop on error
			if state, _ := t.pipeline.GetState(); state != nil {
				t.stateStore.Save(StateKey, state)
			}
			t.logger.Error("stop on error", log.Error(err))
			t.lastError = err
			go t.Stop()
		case <-t.stop:
			// guarding goroutine by stopped channel to prevent partial state saving
			// but in extreme situations like os crash still need StateStore.Save to be atomic.
			close(t.stopped)
			return
		case <-ticker.C:
			state, done := t.pipeline.GetState()
			if state != nil {
				if err := t.stateStore.Save(StateKey, state); err != nil {
					t.logger.Info("failed save state", log.Error(err))
				}
			}
			if done {
				go t.Stop()
			}
		}
	}
}

func (t *DefaultTask) start() (err error) {
	state, err := t.loadInitState()
	if err != nil {
		return
	}
	if err = t.pipeline.SetState(state); err != nil {
		return
	}
	if err = t.pipeline.Start(); err != nil {
		return
	}
	go t.syncState()
	return
}

func (t *DefaultTask) Start() (err error) {
	t.configureLog()
	defer t.logger.Sync()

	t.logger.Info("initializing task")
	t.startPprof()
	if err = metrics.InitTaskMetricsSingleton(t.config, t.logger); err != nil {
		err = errors.Wrap(err, "fail init task metrics")
		return
	}

	builder.InitComponentBuilder(t.logger)

	if err = t.configure(); err != nil {
		s := "fail to configure task"
		err = errors.Wrap(err, s)
		t.logger.Error(s, log.Error(err))
		return err
	}

	if err = t.start(); err != nil {
		s := "failed to start task"
		err = errors.Wrap(err, s)
		t.logger.Error(s, log.String("id", t.ID), log.Error(err))
		return err
	}
	t.logger.Info("task started", log.String("id", t.ID))

	select {
	case <-utils.SignalQuit():
		t.logger.Info("stopping task", log.String("reason", "sig quit"))
		t.Stop()
	case <-t.stopped:
	}

	t.logger.Info("start function exited from blocking", log.Error(t.lastError))
	return t.lastError
}

func (t *DefaultTask) loadInitState() (state []byte, err error) {
	if state, err = t.stateStore.Load(StateKey); err == nil {
		t.logger.Info("starts with", log.String("state", string(state)))
		return
	}
	return
}

func (t *DefaultTask) configureLog() {
	if logPath, _ := utils.GetStringFromConfig(t.config, "$.LogPath"); len(logPath) > 0 {
		for _, option := range log.DefaultOptions {
			option.Filename = filepath.Join(logPath, option.Filename)
		}
	}
	t.logger = log.NewTeeWithRotate(log.DefaultOptions)
}

func (t *DefaultTask) GetID() string {
	return t.ID
}

func (t *DefaultTask) Stop() {
	t.stopOnce.Do(func() {
		t.pipeline.Stop()
		close(t.stop)
	})
	<-t.stopped
	t.logger.Info("task stopped")
}

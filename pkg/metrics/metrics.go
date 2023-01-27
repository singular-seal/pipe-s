package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/log"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	DefaultMetricsPort               = 9148
	DefaultStatisticsIntervalSeconds = 10
	TaskDelayUpdateModMask           = 1023

	TaskQPSGaugeName   = "task_qps"
	TaskDelayGaugeName = "task_delay"
)

// MetricsInstance is the Global TaskMetrics singleton
var MetricsInstance *TaskMetrics

func (m *TaskMetrics) Configure(config core.StringMap) (err error) {
	if m.port, err = utils.GetIntFromConfig(config, "$.Port"); err != nil {
		return
	}
	if m.port == 0 {
		m.port = DefaultMetricsPort
	}

	m.labelNames = []string{"task_name"}
	m.labelValues = []string{m.taskName}

	m.taskQPSGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: TaskQPSGaugeName,
			Help: strings.ReplaceAll(TaskQPSGaugeName, "_", " "),
		},
		m.labelNames,
	)
	prometheus.MustRegister(m.taskQPSGauge)

	m.taskDelayGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: TaskDelayGaugeName,
			Help: strings.ReplaceAll(TaskDelayGaugeName, "_", " "),
		},
		m.labelNames,
	)
	prometheus.MustRegister(m.taskDelayGauge)

	return
}

type TaskMetrics struct {
	*core.BaseComponent
	taskName       string
	port           int
	labelNames     []string
	labelValues    []string
	taskQPSGauge   *prometheus.GaugeVec
	taskDelayGauge *prometheus.GaugeVec
	eventCount     int64
}

func (m *TaskMetrics) AddEventCount() {
	atomic.AddInt64(&m.eventCount, 1)
}

func AddEventCount() {
	if MetricsInstance == nil {
		return
	}
	MetricsInstance.AddEventCount()
}

func (m *TaskMetrics) SetTaskDelay(delay float64) {
	m.taskDelayGauge.WithLabelValues(m.labelValues...).Set(delay)
}

func UpdateTaskBinlogDelay(positionTimestamp uint32) {
	if MetricsInstance == nil {
		return
	}
	// avoid frequently calculate time
	if MetricsInstance.eventCount&TaskDelayUpdateModMask != 0 {
		return
	}

	MetricsInstance.SetTaskDelay(float64(time.Now().Unix() - int64(positionTimestamp)))
}

func (m *TaskMetrics) makeStatistics() {
	m.taskQPSGauge.WithLabelValues(m.labelValues...).Set(float64(atomic.LoadInt64(&m.eventCount) / DefaultStatisticsIntervalSeconds))
	atomic.StoreInt64(&m.eventCount, 0)
}

func NewTaskMetrics(taskName string) *TaskMetrics {
	return &TaskMetrics{
		BaseComponent: core.NewBaseComponent(),
		taskName:      taskName,
	}
}

func (m *TaskMetrics) Start() (err error) {
	go func() {
		m.GetLogger().Info("starting metrics", log.Int("port", m.port))
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil)
	}()

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(DefaultStatisticsIntervalSeconds))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.makeStatistics()
			}
		}
	}()
	return nil
}

func InitTaskMetricsSingleton(taskConfig core.StringMap, logger *log.Logger) (err error) {
	if MetricsInstance != nil {
		return
	}
	var taskName string
	if taskName, err = utils.GetStringFromConfig(taskConfig, "$.ID"); err != nil {
		return
	}
	mc, err1 := utils.GetConfigFromConfig(taskConfig, "$.Metrics")
	// no metrics config, just return
	if err1 != nil {
		return nil
	}
	MetricsInstance = NewTaskMetrics(taskName)
	if err = MetricsInstance.Configure(mc); err != nil {
		return
	}
	MetricsInstance.SetLogger(logger)
	return MetricsInstance.Start()
}

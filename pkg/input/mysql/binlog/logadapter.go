package binlog

import (
	lib_log "github.com/siddontang/go-log/log"
	"github.com/singular-seal/pipe-s/pkg/log"
	"sync"
)

var initGoMysqlLoggerOnce sync.Once

func initGoMysqlLogger(logger *log.Logger) {
	initGoMysqlLoggerOnce.Do(func() {
		logAdapter := lib_log.NewDefault(&GoMysqlLoggerAdapter{logger: logger})
		lib_log.SetDefaultLogger(logAdapter)
	})
}

// GoMysqlLoggerAdapter is adapter to pass logs from go mysql logger to our logger.
type GoMysqlLoggerAdapter struct {
	logger *log.Logger
}

func (h *GoMysqlLoggerAdapter) Write(p []byte) (n int, err error) {
	// remove trailing newline
	if len(p) > 0 && p[len(p)-1] == '\n' {
		p = p[:len(p)-1]
	}
	h.logger.Info("go_mysql", log.String("msg", string(p)))
	return len(p), nil
}

func (h *GoMysqlLoggerAdapter) Close() error {
	return nil
}

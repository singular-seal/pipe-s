package binlog

import (
	lib_log "github.com/siddontang/go-log/log"
	"github.com/singular-seal/pipe-s/pkg/log"
)

func initGoMysqlLogger(logger *log.Logger) {
	logAdapter := lib_log.NewDefault(&GoMysqlLoggerAdapter{logger: logger})
	lib_log.SetDefaultLogger(logAdapter)
}

// GoMysqlLoggerAdapter is adapter to pass logs from go mysql logger to our logger.
type GoMysqlLoggerAdapter struct {
	logger *log.Logger
}

func (a *GoMysqlLoggerAdapter) Write(p []byte) (n int, err error) {
	if len(p) > 0 && p[len(p)-1] == '\n' {
		p = p[:len(p)-1]
	}
	a.logger.Info("go_mysql", log.String("msg", string(p)))
	return len(p), nil
}

func (a *GoMysqlLoggerAdapter) Close() error {
	return nil
}

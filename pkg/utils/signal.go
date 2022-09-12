package utils

import (
	"os"
	"os/signal"
	"syscall"
)

func SignalQuit() chan os.Signal {
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return quit
}

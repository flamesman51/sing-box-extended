package mtproxy

import (
	"fmt"

	"github.com/sagernet/sing/common/logger"

	"github.com/dolonet/mtg-multi/mtglib"
)

type LoggerAdapter struct {
	logger logger.Logger
}

func NewLoggerAdapter(logger logger.Logger) *LoggerAdapter {
	return &LoggerAdapter{logger}
}

func (l *LoggerAdapter) Named(name string) mtglib.Logger {
	return l
}

func (l *LoggerAdapter) BindInt(name string, value int) mtglib.Logger {
	return l
}

func (l *LoggerAdapter) BindStr(name, value string) mtglib.Logger {
	return l
}

func (l *LoggerAdapter) BindJSON(name, value string) mtglib.Logger {
	return l
}

func (l *LoggerAdapter) Printf(format string, args ...any) {
	l.logger.Info(fmt.Sprintf(format, args...))
}

func (l *LoggerAdapter) Info(msg string) {
	l.logger.Info(msg)
}

func (l *LoggerAdapter) InfoError(msg string, err error) {
	l.logger.Error(msg, err)
}

func (l *LoggerAdapter) Warning(msg string) {
	l.logger.Warn(msg)
}

func (l *LoggerAdapter) WarningError(msg string, err error) {
	l.logger.Warn(msg, err)
}

func (l *LoggerAdapter) Debug(msg string) {
	l.logger.Debug(msg)
}

func (l *LoggerAdapter) DebugError(msg string, err error) {
	l.logger.Debug(msg, err)
}

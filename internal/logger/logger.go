package logger

import (
	"os"
	"sync"

	"github.com/rs/zerolog"
)

var (
	instance   zerolog.Logger
	loggerOnce sync.Once
)

// GetLogger returns the global logger instance.
func GetLogger() zerolog.Logger {
	loggerOnce.Do(func() {
		instance = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}).With().Timestamp().Logger()
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	})
	return instance
}

// Init initializes the logger with the specified level.
func Init(level string) {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	loggerOnce.Do(func() {
		instance = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}).With().Timestamp().Logger().Level(logLevel)
		zerolog.SetGlobalLevel(logLevel)
	})
}

// SetLevel changes the log level dynamically.
func SetLevel(level string) {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)
	instance = instance.Level(logLevel)
}

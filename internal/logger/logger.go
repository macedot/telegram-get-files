package logger

import (
	"os"
	"sync"

	"github.com/rs/zerolog"
)

var (
	instance   zerolog.Logger
	initOnce   sync.Once
	levelMutex sync.RWMutex
)

// GetLogger returns the global logger instance.
// If Init() hasn't been called, initializes with defaults.
func GetLogger() zerolog.Logger {
	initOnce.Do(func() {
		levelMutex.Lock()
		instance = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}).With().Timestamp().Logger()
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		levelMutex.Unlock()
	})

	levelMutex.RLock()
	defer levelMutex.RUnlock()
	return instance
}

// Init initializes the logger with the specified level.
// This should be called once at application startup.
// Safe to call multiple times - subsequent calls update the level.
func Init(level string) {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	initOnce.Do(func() {
		levelMutex.Lock()
		instance = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		}).With().Timestamp().Logger()
		zerolog.SetGlobalLevel(logLevel)
		levelMutex.Unlock()
	})

	levelMutex.Lock()
	instance = instance.Level(logLevel)
	zerolog.SetGlobalLevel(logLevel)
	levelMutex.Unlock()
}

// SetLevel changes the log level dynamically.
// Thread-safe for concurrent use.
func SetLevel(level string) {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	levelMutex.Lock()
	defer levelMutex.Unlock()

	zerolog.SetGlobalLevel(logLevel)
	instance = instance.Level(logLevel)
}

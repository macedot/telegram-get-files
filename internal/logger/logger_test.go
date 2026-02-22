package logger

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLogger(t *testing.T) {
	logger := GetLogger()
	assert.NotNil(t, logger)
}

func TestGetLogger_MultipleCalls(t *testing.T) {
	logger1 := GetLogger()
	logger2 := GetLogger()
	assert.Equal(t, logger1, logger2)
}

func TestInit_Levels(t *testing.T) {
	tests := []string{"debug", "info", "warn", "error", "trace"}

	for _, level := range tests {
		t.Run(level, func(t *testing.T) {
			Init(level)
			logger := GetLogger()
			assert.NotNil(t, logger)
		})
	}
}

func TestInit_InvalidLevel(t *testing.T) {
	Init("invalid_level")
	logger := GetLogger()
	assert.NotNil(t, logger)
}

func TestSetLevel(t *testing.T) {
	SetLevel("debug")
	logger := GetLogger()
	assert.NotNil(t, logger)

	SetLevel("error")
	logger = GetLogger()
	assert.NotNil(t, logger)
}

func TestLogger_ConcurrentAccess(t *testing.T) {
	var wg sync.WaitGroup
	iterations := 100

	for i := 0; i < iterations; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_ = GetLogger()
		}()
		go func() {
			defer wg.Done()
			Init("info")
		}()
		go func() {
			defer wg.Done()
			SetLevel("debug")
		}()
	}

	wg.Wait()
}

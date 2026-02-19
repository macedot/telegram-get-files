package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLogger(t *testing.T) {
	// GetLogger should return a logger instance
	logger := GetLogger()
	assert.NotNil(t, logger)
}

func TestGetLogger_MultipleCalls(t *testing.T) {
	// Multiple calls should return the same instance
	logger1 := GetLogger()
	logger2 := GetLogger()
	assert.Equal(t, logger1, logger2)
}

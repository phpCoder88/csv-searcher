package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateLogger(t *testing.T) {
	logger := createLogger()
	assert.IsType(t, zap.Logger{}, *logger)
}

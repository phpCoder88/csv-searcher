package csvquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsOperator(t *testing.T) {
	assert.True(t, IsOperator("AND"))
	assert.True(t, IsOperator("and"))
	assert.True(t, IsOperator("OR"))
	assert.True(t, IsOperator("or"))
	assert.False(t, IsOperator("CONST"))
}

func TestGetPriority(t *testing.T) {
	assert.Equal(t, 2, GetPriority(AndOperator))
	assert.Equal(t, 1, GetPriority(OrOperator))
	assert.Less(t, GetPriority(OrOperator), GetPriority(AndOperator))
	assert.Equal(t, -1, GetPriority("COND"))
}

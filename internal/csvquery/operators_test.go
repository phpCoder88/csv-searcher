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

func TestIsSameOperator(t *testing.T) {
	assert.True(t, IsSameOperator("AND", AndOperator))
	assert.True(t, IsSameOperator("OR", OrOperator))
	assert.False(t, IsSameOperator("IN", OrOperator))
	assert.False(t, IsSameOperator("LIKE", AndOperator))
}

func TestCalc(t *testing.T) {
	assert.True(t, Calc(true, true, AndOperator))
	assert.False(t, Calc(true, false, AndOperator))
	assert.False(t, Calc(false, true, AndOperator))
	assert.False(t, Calc(false, false, AndOperator))

	assert.True(t, Calc(true, true, OrOperator))
	assert.True(t, Calc(true, false, OrOperator))
	assert.True(t, Calc(false, true, OrOperator))
	assert.False(t, Calc(false, false, OrOperator))
}

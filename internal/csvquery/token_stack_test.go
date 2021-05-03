package csvquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWhereTokenStack_Push(t *testing.T) {
	stack := new(WhereTokenStack)

	stack.Push(OpenBracketToken)
	stack.Push(CondToken)
	stack.Push(CloseBracketToken)

	assert.Equal(t, []WhereToken{OpenBracketToken, CondToken, CloseBracketToken}, stack.values)
	assert.True(t, stack.IsTopEqual(CloseBracketToken))
}

func TestWhereTokenStack_Pop(t *testing.T) {
	stack := new(WhereTokenStack)

	_, err := stack.Pop()
	assert.Equal(t, ErrPopEmptyStack, err)

	stack.Push(OpenBracketToken)
	stack.Push(CondToken)

	token, err := stack.Pop()
	assert.NoError(t, err)
	assert.Equal(t, CondToken, token)

	assert.Equal(t, []WhereToken{OpenBracketToken}, stack.values)
	assert.True(t, stack.IsTopEqual(OpenBracketToken))
}

func TestWhereTokenStack_IsEmpty(t *testing.T) {
	stack := new(WhereTokenStack)

	assert.True(t, stack.IsEmpty())
	stack.Push(OpenBracketToken)
	assert.False(t, stack.IsEmpty())
}

func TestWhereTokenStack_IsNotEmpty(t *testing.T) {
	stack := new(WhereTokenStack)

	assert.False(t, stack.IsNotEmpty())
	stack.Push(OpenBracketToken)
	assert.True(t, stack.IsNotEmpty())
}

func TestWhereTokenStack_IsTopEqual(t *testing.T) {
	stack := new(WhereTokenStack)

	assert.False(t, stack.IsTopEqual(OpenBracketToken))
	stack.Push(OpenBracketToken)
	assert.True(t, stack.IsTopEqual(OpenBracketToken))
	assert.False(t, stack.IsTopEqual(CloseBracketToken))
}

package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringStack_Push(t *testing.T) {
	stack := new(StringStack)

	stack.Push("(")
	stack.Push("(")
	stack.Push(")")

	assert.Equal(t, []string{"(", "(", ")"}, stack.values)
	assert.True(t, stack.IsTopEqual(")"))
}

func TestStringStack_Pop(t *testing.T) {
	stack := new(StringStack)

	_, err := stack.Pop()
	assert.Equal(t, ErrPopEmptyStack, err)

	stack.Push("(")
	stack.Push("TestString")

	value, err := stack.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "TestString", value)

	assert.Equal(t, []string{"("}, stack.values)
	assert.True(t, stack.IsTopEqual("("))
}

func TestStringStack_Top(t *testing.T) {
	stack := new(StringStack)

	_, err := stack.Top()
	assert.Equal(t, ErrPopEmptyStack, err)

	stack.Push("(")
	stack.Push("TestString")

	value, err := stack.Top()
	assert.NoError(t, err)
	assert.Equal(t, "TestString", value)

	assert.Equal(t, []string{"(", "TestString"}, stack.values)
	assert.True(t, stack.IsTopEqual("TestString"))
}

func TestStringStack_IsEmpty(t *testing.T) {
	stack := new(StringStack)

	assert.True(t, stack.IsEmpty())
	stack.Push("TEST")
	assert.False(t, stack.IsEmpty())
}

func TestStringStack_IsNotEmpty(t *testing.T) {
	stack := new(StringStack)

	assert.False(t, stack.IsNotEmpty())
	stack.Push("NOT EMPTY")
	assert.True(t, stack.IsNotEmpty())
}

func TestStringStack_IsTopEqual(t *testing.T) {
	stack := new(StringStack)

	assert.False(t, stack.IsTopEqual("Value"))
	stack.Push("Value")
	assert.True(t, stack.IsTopEqual("Value"))
	assert.False(t, stack.IsTopEqual("NewValue"))
}

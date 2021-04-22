package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringQueue_Push(t *testing.T) {
	queue := new(StringQueue)

	queue.Push("Value one")
	queue.Push("Value two")
	queue.Push("Value three")

	assert.Equal(t, []string{"Value one", "Value two", "Value three"}, queue.values)
}

func TestStringQueue_Pop(t *testing.T) {
	queue := new(StringQueue)

	_, err := queue.Pop()
	assert.Equal(t, ErrPopEmptyQueue, err)

	queue.Push("Value one")
	queue.Push("Value two")
	queue.Push("Value three")

	value, err := queue.Pop()
	assert.NoError(t, err)
	assert.Equal(t, "Value one", value)

	assert.Equal(t, []string{"Value two", "Value three"}, queue.values)
}

func TestStringQueue_IsEmpty(t *testing.T) {
	queue := new(StringQueue)

	assert.True(t, queue.IsEmpty())
	queue.Push("Value one")
	assert.False(t, queue.IsEmpty())
}

func TestStringQueue_IsNotEmpty(t *testing.T) {
	queue := new(StringQueue)

	assert.False(t, queue.IsNotEmpty())
	queue.Push("Value one")
	queue.Push("Value two")
	assert.True(t, queue.IsNotEmpty())
}

func TestStringQueue_Clear(t *testing.T) {
	queue := new(StringQueue)

	queue.Push("Value one")
	queue.Push("Value two")
	queue.Push("Value three")

	assert.Equal(t, []string{"Value one", "Value two", "Value three"}, queue.values)

	queue.Clear()
	assert.Empty(t, queue.values)
}

func TestStringQueue_PopAllAndClear(t *testing.T) {
	queue := new(StringQueue)

	values := queue.PopAllAndClear()
	assert.Empty(t, values)

	queue.Push("Value one")
	queue.Push("Value two")
	queue.Push("Value three")

	assert.Equal(t, []string{"Value one", "Value two", "Value three"}, queue.values)

	values = queue.PopAllAndClear()
	assert.Equal(t, []string{}, queue.values)
	assert.Equal(t, []string{"Value one", "Value two", "Value three"}, values)
}

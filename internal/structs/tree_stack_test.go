package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTreeStack_Push(t *testing.T) {
	stack := TreeStack{}

	tree1 := NewTree("Tree one", nil, nil)
	tree2 := NewTree("Tree two", nil, nil)

	stack.Push(tree1)
	stack.Push(tree2)

	assert.Equal(t, []*Tree{tree1, tree2}, stack.values)
}

func TestTreeStack_Pop(t *testing.T) {
	stack := TreeStack{}

	tree, err := stack.Pop()
	assert.Equal(t, ErrPopEmptyStack, err)
	assert.Empty(t, tree)

	tree1 := NewTree("Tree one", nil, nil)
	tree2 := NewTree("Tree two", nil, nil)

	stack.Push(tree1)
	stack.Push(tree2)

	tree, err = stack.Pop()
	assert.NoError(t, err)
	assert.Equal(t, tree2, tree)
	assert.Equal(t, []*Tree{tree1}, stack.values)
}

package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTree(t *testing.T) {
	childLeft := NewTree("Child Left", nil, nil)
	childRight := NewTree("Child Right", nil, nil)
	tree := NewTree("Parent", childLeft, childRight)

	assert.Equal(t, childLeft, tree.LeftChild())
	assert.Equal(t, childRight, tree.RightChild())

	assert.Equal(t, "Child Left", tree.LeftChild().GetValue())
	assert.Equal(t, "Child Right", tree.RightChild().GetValue())
}

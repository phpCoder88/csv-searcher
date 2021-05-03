package structs

// Tree contains a tree node.
type Tree struct {
	left  *Tree
	right *Tree
	value interface{}
}

// NewTree return new tree node.
func NewTree(value interface{}, left, right *Tree) *Tree {
	return &Tree{
		value: value,
		left:  left,
		right: right,
	}
}

// LeftChild returns the left tree child node.
func (t *Tree) LeftChild() *Tree {
	return t.left
}

// RightChild returns the right tree child node.
func (t *Tree) RightChild() *Tree {
	return t.right
}

// GetValue returns the node value.
func (t *Tree) GetValue() interface{} {
	return t.value
}

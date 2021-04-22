package structs

type Tree struct {
	left  *Tree
	right *Tree
	value interface{}
}

func NewTree(value interface{}, left, right *Tree) *Tree {
	return &Tree{
		value: value,
		left:  left,
		right: right,
	}
}

func (t *Tree) LeftChild() *Tree {
	return t.left
}

func (t *Tree) RightChild() *Tree {
	return t.right
}

func (t *Tree) GetValue() interface{} {
	return t.value
}

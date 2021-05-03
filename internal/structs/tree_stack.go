package structs

// TreeStack contains stack of tree pointers.
type TreeStack struct {
	values []*Tree
}

// Push adds tree pointer value to the stack.
func (s *TreeStack) Push(value *Tree) {
	s.values = append(s.values, value)
}

// Pop pops the tree element off the end of the stack
// and ErrPopEmptyStack error if the stack is empty.
func (s *TreeStack) Pop() (*Tree, error) {
	if len(s.values) == 0 {
		return nil, ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

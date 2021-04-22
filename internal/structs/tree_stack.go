package structs

type TreeStack struct {
	values []*Tree
}

func (s *TreeStack) Push(value *Tree) {
	s.values = append(s.values, value)
}
func (s *TreeStack) Pop() (*Tree, error) {
	if len(s.values) == 0 {
		return nil, ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

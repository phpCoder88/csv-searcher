package structs

type StringStack struct {
	values []string
}

func (s *StringStack) Push(value string) {
	s.values = append(s.values, value)
}

func (s *StringStack) Pop() (string, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

func (s *StringStack) Top() (string, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	return s.values[len(s.values)-1], nil
}

func (s *StringStack) IsTopEqual(val string) bool {
	return !s.IsEmpty() && s.values[len(s.values)-1] == val
}

func (s *StringStack) IsEmpty() bool {
	return len(s.values) == 0
}

func (s *StringStack) IsNotEmpty() bool {
	return len(s.values) != 0
}

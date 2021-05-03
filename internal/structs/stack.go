package structs

// StringStack contains stack of strings.
type StringStack struct {
	values []string
}

// Push adds string value to the stack.
func (s *StringStack) Push(value string) {
	s.values = append(s.values, value)
}

// Pop pops the string element off the end of the stack
// and ErrPopEmptyStack error if the stack is empty.
func (s *StringStack) Pop() (string, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

// Top returns the string element off the end of the stack
// and a ErrPopEmptyStack error if the stack is empty.
func (s *StringStack) Top() (string, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	return s.values[len(s.values)-1], nil
}

// IsTopEqual return true if the last element of the stack is equal to val and false otherwise.
func (s *StringStack) IsTopEqual(val string) bool {
	return !s.IsEmpty() && s.values[len(s.values)-1] == val
}

// IsEmpty return true if the stack is empty and false otherwise.
func (s *StringStack) IsEmpty() bool {
	return len(s.values) == 0
}

// IsNotEmpty return true if the stack is not empty and false otherwise.
func (s *StringStack) IsNotEmpty() bool {
	return len(s.values) != 0
}

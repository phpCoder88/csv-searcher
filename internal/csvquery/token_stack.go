package csvquery

import "errors"

type WhereToken string

const (
	OpenBracketToken  WhereToken = "("
	CloseBracketToken WhereToken = ")"
	CondToken         WhereToken = "COND"
	BinaryOpToken     WhereToken = "OP"
)

type WhereTokenStack struct {
	values []WhereToken
}

var ErrPopEmptyStack = errors.New("stack is empty")

func (s *WhereTokenStack) Push(value WhereToken) {
	s.values = append(s.values, value)
}

func (s *WhereTokenStack) Pop() (WhereToken, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

func (s *WhereTokenStack) IsTopEqual(val WhereToken) bool {
	return !s.IsEmpty() && s.values[len(s.values)-1] == val
}

func (s *WhereTokenStack) IsEmpty() bool {
	return len(s.values) == 0
}

func (s *WhereTokenStack) IsNotEmpty() bool {
	return len(s.values) != 0
}

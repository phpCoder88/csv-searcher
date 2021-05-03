package csvquery

import "errors"

// WhereToken describes token type.
type WhereToken string

const (
	// OpenBracketToken returns the opening parenthesis token.
	OpenBracketToken WhereToken = "("
	// CloseBracketToken returns the closing parenthesis token.
	CloseBracketToken WhereToken = ")"
	// CondToken returns the condition token.
	CondToken WhereToken = "COND"
	// BinaryOpToken returns the operation token.
	BinaryOpToken WhereToken = "OP"
)

// WhereTokenStack contains a stack of tokens.
type WhereTokenStack struct {
	values []WhereToken
}

// ErrPopEmptyStack returns error if a token stack is empty.
var ErrPopEmptyStack = errors.New("stack is empty")

// Push adds a token to the stack.
func (s *WhereTokenStack) Push(value WhereToken) {
	s.values = append(s.values, value)
}

// Pop pops the string element off the end of the stack
// and ErrPopEmptyStack error if the stack is empty.
func (s *WhereTokenStack) Pop() (WhereToken, error) {
	if len(s.values) == 0 {
		return "", ErrPopEmptyStack
	}

	lastValue := s.values[len(s.values)-1]
	s.values = s.values[:len(s.values)-1]
	return lastValue, nil
}

// IsTopEqual return true if the last element of the stack is equal to val and false otherwise.
func (s *WhereTokenStack) IsTopEqual(val WhereToken) bool {
	return !s.IsEmpty() && s.values[len(s.values)-1] == val
}

// IsEmpty return true if the stack is empty and false otherwise.
func (s *WhereTokenStack) IsEmpty() bool {
	return len(s.values) == 0
}

// IsNotEmpty return true if the stack is not empty and false otherwise.
func (s *WhereTokenStack) IsNotEmpty() bool {
	return len(s.values) != 0
}

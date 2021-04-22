package structs

import "errors"

var (
	ErrPopEmptyStack = errors.New("stack is empty")
	ErrPopEmptyQueue = errors.New("queue is empty")
)

// Package structs contains queue, stack and tree structs.
package structs

import "errors"

var (
	// ErrPopEmptyStack return error if stack is empty.
	ErrPopEmptyStack = errors.New("stack is empty")
	// ErrPopEmptyQueue return error if queue is empty.
	ErrPopEmptyQueue = errors.New("queue is empty")
)

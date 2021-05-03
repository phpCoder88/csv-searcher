package structs

// StringQueue contains queue of strings.
type StringQueue struct {
	values []string
}

// Push adds string value to the queue.
func (q *StringQueue) Push(value string) {
	q.values = append(q.values, value)
}

// Pop shifts the string element off the beginning of the queue
// or return ErrPopEmptyQueue error if the queue is empty.
func (q *StringQueue) Pop() (string, error) {
	if len(q.values) == 0 {
		return "", ErrPopEmptyQueue
	}

	firstValue := q.values[0]
	q.values = q.values[1:]
	return firstValue, nil
}

// IsEmpty return true if the queue is empty and false otherwise.
func (q *StringQueue) IsEmpty() bool {
	return len(q.values) == 0
}

// IsNotEmpty return true if the queue is not empty and false otherwise.
func (q *StringQueue) IsNotEmpty() bool {
	return !q.IsEmpty()
}

// PopAllAndClear returns all queue elements and clears the queue.
func (q *StringQueue) PopAllAndClear() []string {
	if q.IsEmpty() {
		return nil
	}

	allQueue := make([]string, 0, len(q.values))
	allQueue = append(allQueue, q.values...)
	q.Clear()

	return allQueue
}

// Clear clears the queue.
func (q *StringQueue) Clear() {
	q.values = q.values[:0]
}

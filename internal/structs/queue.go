package structs

type StringQueue struct {
	values []string
}

func (q *StringQueue) Push(value string) {
	q.values = append(q.values, value)
}

func (q *StringQueue) Pop() (string, error) {
	if len(q.values) == 0 {
		return "", ErrPopEmptyQueue
	}

	firstValue := q.values[0]
	q.values = q.values[1:]
	return firstValue, nil
}

func (q *StringQueue) IsEmpty() bool {
	return len(q.values) == 0
}

func (q *StringQueue) IsNotEmpty() bool {
	return !q.IsEmpty()
}

func (q *StringQueue) PopAllAndClear() []string {
	if q.IsEmpty() {
		return nil
	}

	allQueue := make([]string, 0, len(q.values))
	allQueue = append(allQueue, q.values...)
	q.Clear()

	return allQueue
}

func (q *StringQueue) Clear() {
	q.values = q.values[:0]
}

package csvquery

import (
	"strings"

	"github.com/phpCoder88/csv-searcher/internal/structs"
)

// InfixNotation describes list of tokens in infix notation.
type InfixNotation struct {
	tokens []string
	stack  *structs.StringStack
	queue  *structs.StringQueue
}

// NewInfixNotation return new infix notation instance.
func NewInfixNotation() *InfixNotation {
	return &InfixNotation{
		stack: new(structs.StringStack),
		queue: new(structs.StringQueue),
	}
}

// AddToken adds new token.
func (n *InfixNotation) AddToken(token string) {
	n.tokens = append(n.tokens, token)
}

// Size returns count of tokens.
func (n *InfixNotation) Size() int {
	return len(n.tokens)
}

func (n *InfixNotation) isValue(val string) bool {
	return strings.HasPrefix(val, ConditionPrefix)
}

// ToPostfix returns list of tokens in postfix notation.
func (n *InfixNotation) ToPostfix() []string {
	for _, val := range n.tokens {
		if n.isValue(val) {
			n.queue.Push(val)
		} else if IsOperator(val) {
			n.processOperatorCase(val)
		} else if val == "(" {
			n.stack.Push(val)
		} else if val == ")" {
			for n.stack.IsNotEmpty() && !n.stack.IsTopEqual("(") {
				item, _ := n.stack.Pop()
				n.queue.Push(item)
			}
			_, _ = n.stack.Pop()
		}
	}

	for n.stack.IsNotEmpty() {
		item, _ := n.stack.Pop()
		n.queue.Push(item)
	}

	return n.queue.PopAllAndClear()
}

func (n *InfixNotation) processOperatorCase(val string) {
	if last, _ := n.stack.Top(); n.stack.IsEmpty() || n.stack.IsTopEqual("(") {
		n.stack.Push(val)
	} else if GetPriority(LogicalOperator(val)) > GetPriority(LogicalOperator(last)) {
		n.stack.Push(val)
	} else {
		for n.stack.IsNotEmpty() &&
			(!n.stack.IsTopEqual("(") ||
				(IsOperator(last) && GetPriority(LogicalOperator(val)) < GetPriority(LogicalOperator(last)))) {
			elem, _ := n.stack.Pop()
			n.queue.Push(elem)
			last, _ = n.stack.Top()
		}

		n.stack.Push(val)
	}
}

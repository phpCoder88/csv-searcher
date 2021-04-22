package csvquery

import "strings"

type LogicalOperator string

const (
	AndOperator LogicalOperator = "AND"
	OrOperator  LogicalOperator = "OR"
)

var opPriority = map[LogicalOperator]int{
	OrOperator:  1,
	AndOperator: 2,
}

func IsOperator(operator string) bool {
	return strings.EqualFold(operator, string(AndOperator)) || strings.EqualFold(operator, string(OrOperator))
}

func GetPriority(operator LogicalOperator) int {
	if priority, ok := opPriority[operator]; ok {
		return priority
	}

	return -1
}

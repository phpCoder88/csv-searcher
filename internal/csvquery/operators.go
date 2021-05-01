package csvquery

import "strings"

type LogicalOperator string

const (
	AndOperator LogicalOperator = "AND"
	OrOperator  LogicalOperator = "OR"
)

type ComparisonOperator string

const (
	EqualOperator          ComparisonOperator = "="
	NotEqualOperator       ComparisonOperator = "!="
	LessOperator           ComparisonOperator = "<"
	LessOrEqualOperator    ComparisonOperator = "<="
	GreaterOperator        ComparisonOperator = ">"
	GreaterOrEqualOperator ComparisonOperator = ">="
)

var ComparisonOperators = []ComparisonOperator{
	EqualOperator,
	NotEqualOperator,
	LessOperator,
	LessOrEqualOperator,
	GreaterOperator,
	GreaterOrEqualOperator,
}

var opPriority = map[LogicalOperator]int{
	OrOperator:  1,
	AndOperator: 2,
}

func IsOperator(operator string) bool {
	return strings.EqualFold(operator, string(AndOperator)) || strings.EqualFold(operator, string(OrOperator))
}

func IsSameOperator(strOp string, op LogicalOperator) bool {
	return LogicalOperator(strOp) == op
}

func Calc(left, right bool, op LogicalOperator) bool {
	if op == AndOperator {
		return left && right
	}

	return left || right
}

func GetPriority(operator LogicalOperator) int {
	if priority, ok := opPriority[operator]; ok {
		return priority
	}

	return -1
}

package csvquery

import "strings"

// LogicalOperator describes logical operator type.
type LogicalOperator string

const (
	// AndOperator describes AND logical operator.
	AndOperator LogicalOperator = "AND"
	// OrOperator describes OR logical operator.
	OrOperator LogicalOperator = "OR"
)

// ComparisonOperator describes comparison operator type.
type ComparisonOperator string

const (
	// EqualOperator describes equal operator.
	EqualOperator ComparisonOperator = "="
	// NotEqualOperator describes  "not equal" operator.
	NotEqualOperator ComparisonOperator = "!="
	// LessOperator describes less operator
	LessOperator ComparisonOperator = "<"
	// LessOrEqualOperator describes less or equal operator.
	LessOrEqualOperator ComparisonOperator = "<="
	// GreaterOperator describes greater operator.
	GreaterOperator ComparisonOperator = ">"
	// GreaterOrEqualOperator describes greater or equal operator.
	GreaterOrEqualOperator ComparisonOperator = ">="
)

// ComparisonOperators contains list of possible comparison operators.
var ComparisonOperators = []ComparisonOperator{
	EqualOperator,
	NotEqualOperator,
	LessOperator,
	LessOrEqualOperator,
	GreaterOperator,
	GreaterOrEqualOperator,
}

// opPriority contains priority of logical operators.
var opPriority = map[LogicalOperator]int{
	OrOperator:  1,
	AndOperator: 2,
}

// IsOperator returns true if operator is one of the logical operators.
func IsOperator(operator string) bool {
	return strings.EqualFold(operator, string(AndOperator)) || strings.EqualFold(operator, string(OrOperator))
}

// IsSameOperator returns true if strOp and op describes the same operator and false otherwise.
func IsSameOperator(strOp string, op LogicalOperator) bool {
	return LogicalOperator(strOp) == op
}

// Calc calculates binary statement.
func Calc(left, right bool, op LogicalOperator) bool {
	if op == AndOperator {
		return left && right
	}

	return left || right
}

// GetPriority returns priority of the binary operator.
func GetPriority(operator LogicalOperator) int {
	if priority, ok := opPriority[operator]; ok {
		return priority
	}

	return -1
}

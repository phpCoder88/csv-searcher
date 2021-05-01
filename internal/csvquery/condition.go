package csvquery

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type ValueType uint

const (
	TypeNumber ValueType = iota
	TypeString
)

var (
	ErrUnknownValueType          = errors.New("unknown value type of condition value")
	ErrUnknownComparisonOperator = errors.New("unknown comparison operator")
	ErrCastInterfaceToString     = errors.New("can't cast interface to string")
	ErrCastInterfaceToFloat64    = errors.New("can't cast interface to float64")
	ErrConvertToFloat64          = errors.New("can't convert float64")
)

type Condition struct {
	Column    Column
	Op        ComparisonOperator
	ValueType ValueType
	Value     interface{}
}

const ConditionPrefix = "COND"

type ConditionMap map[string]*Condition

func (cm *ConditionMap) Add(cond *Condition) string {
	if condKey, found := cm.exists(cond); found {
		return condKey
	}

	expKey := fmt.Sprintf("%s%d", ConditionPrefix, len(*cm))
	(*cm)[expKey] = cond
	return expKey
}

func (cm *ConditionMap) exists(cond *Condition) (string, bool) {
	for key, condItem := range *cm {
		if *condItem == *cond {
			return key, true
		}
	}

	return "", false
}

func (c *Condition) CheckCondition(value string) (bool, error) {
	if c.ValueType == TypeNumber {
		return c.checkNumberCondition(value)
	} else if c.ValueType == TypeString {
		return c.checkStringCondition(value)
	}

	return false, fmt.Errorf("%w: column: %s, condition value: %s, value Type: %d", ErrUnknownValueType, c.Column, value, c.ValueType)
}

func (c *Condition) checkNumberCondition(value string) (bool, error) {
	if strings.TrimSpace(value) == "" {
		return false, nil
	}

	condValue, ok := c.Value.(float64)
	if !ok {
		return false, fmt.Errorf("%w: column: %s, condition value: %v", ErrCastInterfaceToFloat64, c.Column, c.Value)
	}

	colNumberValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false, fmt.Errorf("%w: column: %s, row value: %v", ErrConvertToFloat64, c.Column, value)
	}

	inDelta := func(num1, num2, delta float64) bool {
		dt := num1 - num2
		if dt < -delta || dt > delta {
			return false
		}
		return true
	}

	switch c.Op {
	case EqualOperator:
		return inDelta(colNumberValue, condValue, math.SmallestNonzeroFloat64), nil
	case NotEqualOperator:
		return !inDelta(colNumberValue, condValue, math.SmallestNonzeroFloat64), nil
	case LessOperator:
		return colNumberValue < condValue, nil
	case LessOrEqualOperator:
		return colNumberValue <= condValue, nil
	case GreaterOperator:
		return colNumberValue > condValue, nil
	case GreaterOrEqualOperator:
		return colNumberValue >= condValue, nil
	}

	return false, fmt.Errorf("%w: column: %s, operator: %s", ErrUnknownComparisonOperator, c.Column, c.Op)
}

func (c *Condition) checkStringCondition(value string) (bool, error) {
	condValue, ok := c.Value.(string)
	if !ok {
		return false, fmt.Errorf("%w: column: %s, condition value: %v", ErrCastInterfaceToString, c.Column, c.Value)
	}

	switch c.Op {
	case EqualOperator:
		return value == condValue, nil
	case NotEqualOperator:
		return value != condValue, nil
	case LessOperator:
		return value < condValue, nil
	case LessOrEqualOperator:
		return value <= condValue, nil
	case GreaterOperator:
		return value > condValue, nil
	case GreaterOrEqualOperator:
		return value >= condValue, nil
	}

	return false, fmt.Errorf("%w: column: %s, operator: %s", ErrUnknownComparisonOperator, c.Column, c.Op)
}

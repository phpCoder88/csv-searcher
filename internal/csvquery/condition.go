package csvquery

import "fmt"

var Operations = []string{
	"=", "<", ">", "<=", ">", "!=",
}

type Operation string

type ValueType uint

const (
	TypeNumber ValueType = iota
	TypeString
)

type Condition struct {
	Column    Column
	Op        Operation
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

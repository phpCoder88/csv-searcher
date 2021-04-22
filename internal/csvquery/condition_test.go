package csvquery

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConditionMap_Add(t *testing.T) {
	condMap := make(ConditionMap)

	cond0 := &Condition{
		Column:    "age",
		Op:        "<=",
		ValueType: TypeNumber,
		Value:     30,
	}

	cond1 := &Condition{
		Column:    "date",
		Op:        ">",
		ValueType: TypeString,
		Value:     "2021-01-01",
	}

	cond2 := &Condition{
		Column:    "age",
		Op:        "<=",
		ValueType: TypeNumber,
		Value:     30,
	}

	cond3 := &Condition{
		Column:    "date",
		Op:        ">",
		ValueType: TypeString,
		Value:     "2021-01-01",
	}

	condKey0 := condMap.Add(cond0)
	condKey1 := condMap.Add(cond1)
	condKey2 := condMap.Add(cond2)
	condKey3 := condMap.Add(cond3)

	assert.Equal(t, fmt.Sprintf("%s%d", ConditionPrefix, 0), condKey0)
	assert.Equal(t, fmt.Sprintf("%s%d", ConditionPrefix, 1), condKey1)
	assert.Equal(t, fmt.Sprintf("%s%d", ConditionPrefix, 0), condKey2)
	assert.Equal(t, fmt.Sprintf("%s%d", ConditionPrefix, 1), condKey3)

	assert.Equal(t, ConditionMap{"COND0": cond0, "COND1": cond1}, condMap)
}

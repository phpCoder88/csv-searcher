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

func TestNumberCondition_CheckCondition(t *testing.T) {
	tests := []struct {
		name     string
		cond     *Condition
		colValue string
		wantRes  bool
	}{
		{
			name:     "'' == 30",
			cond:     &Condition{Column: "age", Op: "=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "",
		},
		{
			name:     "25 == 30",
			cond:     &Condition{Column: "age", Op: "=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
		},
		{
			name:     "30 == 30",
			cond:     &Condition{Column: "age", Op: "=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "30",
			wantRes:  true,
		},
		{
			name:     "25 != 30",
			cond:     &Condition{Column: "age", Op: "!=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
			wantRes:  true,
		},
		{
			name:     "30 != 30",
			cond:     &Condition{Column: "age", Op: "!=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "30",
		},
		{
			name:     "25 <= 30",
			cond:     &Condition{Column: "age", Op: "<=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
			wantRes:  true,
		},
		{
			name:     "35 <= 30",
			cond:     &Condition{Column: "age", Op: "<=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "35",
		},
		{
			name:     "35 >= 30",
			cond:     &Condition{Column: "age", Op: ">=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "35",
			wantRes:  true,
		},
		{
			name:     "25 >= 30",
			cond:     &Condition{Column: "age", Op: ">=", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
		},

		{
			name:     "25 < 30",
			cond:     &Condition{Column: "age", Op: "<", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
			wantRes:  true,
		},
		{
			name:     "35 < 30",
			cond:     &Condition{Column: "age", Op: "<", ValueType: TypeNumber, Value: float64(30)},
			colValue: "35",
		},

		{
			name:     "35 > 30",
			cond:     &Condition{Column: "age", Op: ">", ValueType: TypeNumber, Value: float64(30)},
			colValue: "35",
			wantRes:  true,
		},
		{
			name:     "25 > 30",
			cond:     &Condition{Column: "age", Op: ">", ValueType: TypeNumber, Value: float64(30)},
			colValue: "25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.cond.CheckCondition(tt.colValue)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantRes, result)
		})
	}
}

func TestStringCondition_CheckCondition(t *testing.T) {
	tests := []struct {
		name     string
		cond     *Condition
		colValue string
		wantRes  bool
	}{
		{
			name:     "abcd == abc",
			cond:     &Condition{Column: "str", Op: "=", ValueType: TypeString, Value: "abc"},
			colValue: "abcd",
		},
		{
			name:     "abc == abc",
			cond:     &Condition{Column: "str", Op: "=", ValueType: TypeString, Value: "abc"},
			colValue: "abc",
			wantRes:  true,
		},
		{
			name:     "abcd != abc",
			cond:     &Condition{Column: "str", Op: "!=", ValueType: TypeString, Value: "abc"},
			colValue: "abcd",
			wantRes:  true,
		},
		{
			name:     "abc != abc",
			cond:     &Condition{Column: "str", Op: "!=", ValueType: TypeString, Value: "abc"},
			colValue: "abc",
		},
		{
			name:     "abc <= bcd",
			cond:     &Condition{Column: "str", Op: "<=", ValueType: TypeString, Value: "bcd"},
			colValue: "abc",
			wantRes:  true,
		},
		{
			name:     "bcd <= abc",
			cond:     &Condition{Column: "str", Op: "<=", ValueType: TypeString, Value: "abc"},
			colValue: "bcd",
		},
		{
			name:     "bcd >= abc",
			cond:     &Condition{Column: "str", Op: ">=", ValueType: TypeString, Value: "abc"},
			colValue: "bcd",
			wantRes:  true,
		},
		{
			name:     "abc >= bcd",
			cond:     &Condition{Column: "str", Op: ">=", ValueType: TypeString, Value: "bcd"},
			colValue: "abc",
		},

		{
			name:     "bcd < def",
			cond:     &Condition{Column: "str", Op: "<", ValueType: TypeString, Value: "def"},
			colValue: "bcd",
			wantRes:  true,
		},
		{
			name:     "def < bcd",
			cond:     &Condition{Column: "str", Op: "<", ValueType: TypeString, Value: "bcd"},
			colValue: "def",
		},

		{
			name:     "def > bcd",
			cond:     &Condition{Column: "str", Op: ">", ValueType: TypeString, Value: "bcd"},
			colValue: "def",
			wantRes:  true,
		},
		{
			name:     "bcd > def",
			cond:     &Condition{Column: "str", Op: ">", ValueType: TypeString, Value: "def"},
			colValue: "bcd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.cond.CheckCondition(tt.colValue)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantRes, result)
		})
	}
}

func TestConditionError_CheckCondition(t *testing.T) {
	tests := []struct {
		name     string
		cond     *Condition
		colValue string
		wantErr  error
	}{
		{
			name:     "Unknown value type",
			cond:     &Condition{Column: "age", Op: ">", ValueType: ValueType(50), Value: float64(25)},
			colValue: "25",
			wantErr:  ErrUnknownValueType,
		},
		{
			name:     "can't cast interface to float64",
			cond:     &Condition{Column: "age", Op: ">", ValueType: TypeNumber, Value: "twenty five"},
			colValue: "25",
			wantErr:  ErrCastInterfaceToFloat64,
		},
		{
			name:     "can't convert string to float64",
			cond:     &Condition{Column: "age", Op: ">", ValueType: TypeNumber, Value: float64(25)},
			colValue: "twenty five",
			wantErr:  ErrConvertToFloat64,
		},
		{
			name:     "unknown number comparison operator",
			cond:     &Condition{Column: "age", Op: "==", ValueType: TypeNumber, Value: float64(25)},
			colValue: "25",
			wantErr:  ErrUnknownComparisonOperator,
		},
		{
			name:     "unknown string comparison operator",
			cond:     &Condition{Column: "birthDate", Op: "==", ValueType: TypeString, Value: "2021-01-01"},
			colValue: "2021-01-01",
			wantErr:  ErrUnknownComparisonOperator,
		},
		{
			name:     "can't cast interface to string",
			cond:     &Condition{Column: "age", Op: "==", ValueType: TypeString, Value: float64(25)},
			colValue: "2021-01-01",
			wantErr:  ErrCastInterfaceToString,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.cond.CheckCondition(tt.colValue)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

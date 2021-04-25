package csvquery

import (
	"fmt"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/phpCoder88/csv-searcher/internal/structs"

	"github.com/stretchr/testify/assert"
)

func TestWhereParser(t *testing.T) {
	cond1 := &Condition{Column: "age", Op: "<=", ValueType: TypeNumber, Value: float64(54)}
	cond2 := &Condition{Column: "country", Op: "=", ValueType: TypeString, Value: "Europe"}
	cond3 := &Condition{Column: "company", Op: "=", ValueType: TypeString, Value: `OOO "Company Name"`}

	tests := []struct {
		where       string
		wantError   error
		wantResult  *structs.Tree
		wantColumns map[Column]int
	}{
		{
			where:     "",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "age",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "age!= 54)",
			wantError: ErrIncorrectBracketPosition,
		},
		{
			where:     "(age <54",
			wantError: ErrIncorrectBracketPosition,
		},
		{
			where:     "age == 54",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "date <= '2021-04-02",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "(age<54 ABS date >= '2021-04-02' )",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "age <= 54 AND",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     "age gt 54",
			wantError: ErrIncorrectQuery,
		},
		{
			where:     `company = "OOO "Company Name""`,
			wantError: ErrIncorrectQuery,
		},
		{
			where:       "age <= 54",
			wantError:   nil,
			wantResult:  structs.NewTree(cond1, nil, nil),
			wantColumns: map[Column]int{"age": 0},
		},
		{
			where:       `company = "OOO \"Company Name\""`,
			wantError:   nil,
			wantResult:  structs.NewTree(cond3, nil, nil),
			wantColumns: map[Column]int{"company": 0},
		},
		{
			where:       "age <= 54 or country = 'Europe'",
			wantError:   nil,
			wantResult:  structs.NewTree("OR", structs.NewTree(cond1, nil, nil), structs.NewTree(cond2, nil, nil)),
			wantColumns: map[Column]int{"age": 0, "country": 0},
		},
		{
			where:       "age <= 54 or (country = 'Europe')",
			wantError:   nil,
			wantResult:  structs.NewTree("OR", structs.NewTree(cond1, nil, nil), structs.NewTree(cond2, nil, nil)),
			wantColumns: map[Column]int{"age": 0, "country": 0},
		},
	}

	logger := zaptest.NewLogger(t)

	for _, tt := range tests {
		t.Run(tt.where, func(t *testing.T) {
			parser := NewWhereParser(tt.where, logger)
			columns, tree, err := parser.Parse()
			fmt.Println(columns)

			if err != nil {
				assert.Equal(t, tt.wantError, err)
				assert.Empty(t, tree)
				return
			}

			assert.Equal(t, tt.wantColumns, columns)
			assert.Condition(t, func() bool {
				return sameTree(tt.wantResult, tree)
			})
		})
	}
}

func sameTree(expected, actual *structs.Tree) bool {
	if expected == nil && actual == nil {
		return true
	}

	if (expected == nil && actual != nil) || (expected != nil && actual == nil) {
		return false
	}

	res := sameTree(expected.LeftChild(), actual.LeftChild())
	if !res {
		return res
	}

	expVal, expOk := expected.GetValue().(string)
	actVal, actOk := actual.GetValue().(string)
	if (expOk != actOk) || (expOk && expVal != actVal) {
		return false
	}

	if !expOk {
		expVal, expOk := expected.GetValue().(*Condition)
		actVal, actOk := actual.GetValue().(*Condition)

		if (expOk != actOk) || (expOk && *expVal != *actVal) {
			return false
		}
	}

	res = sameTree(expected.RightChild(), actual.RightChild())
	return res
}

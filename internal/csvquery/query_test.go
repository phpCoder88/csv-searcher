package csvquery

import (
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/phpCoder88/csv-searcher/internal/structs"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	cond1 := &Condition{Column: "age", Op: "=", ValueType: TypeNumber, Value: float64(33)}

	tests := []struct {
		query      string
		wantResult *Query
	}{
		{
			query: "select * from users",
			wantResult: &Query{
				query:       "select * from users",
				Select:      Columns{"*"},
				From:        Tables{"users"},
				Where:       nil,
				UsedColumns: nil,
				cursor:      19,
			},
		},
		{
			query: "select name,age from users where age = 33",
			wantResult: &Query{
				query:       "select name,age from users where age = 33",
				Select:      Columns{"name", "age"},
				From:        Tables{"users"},
				Where:       structs.NewTree(cond1, nil, nil),
				UsedColumns: QueryColumns{"name", "age"},
				cursor:      33,
			},
		},
	}
	logger := zaptest.NewLogger(t)

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			query := NewQuery(tt.query, logger)
			err := query.Parse()

			assert.NoError(t, err)
			assert.Equal(t, tt.wantResult.query, query.query)
			assert.Equal(t, tt.wantResult.Select, query.Select)
			assert.Equal(t, tt.wantResult.From, query.From)
			assert.Equal(t, tt.wantResult.cursor, query.cursor)
			assert.Equal(t, tt.wantResult.UsedColumns, query.UsedColumns)

			assert.Condition(t, func() bool {
				return sameTree(tt.wantResult.Where, query.Where)
			})
		})
	}
}

func TestQuery_Errors(t *testing.T) {
	tests := []struct {
		query     string
		wantError error
	}{
		{
			query:     "select",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select ,name",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age from",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age from users,",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age, from users",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name ,,  age  from users",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name , age  from users,,roles",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age from users where ",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select name,age from users where age",
			wantError: ErrIncorrectQuery,
		},
		{
			query:     "select *, age, * from users",
			wantError: ErrTooManyStarColumns,
		},
	}
	logger := zaptest.NewLogger(t)

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			query := NewQuery(tt.query, logger)
			err := query.Parse()

			assert.Equal(t, tt.wantError, err)
		})
	}
}

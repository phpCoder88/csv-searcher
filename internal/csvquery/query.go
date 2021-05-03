// Package csvquery parse and analyzes sql like query string.
package csvquery

import (
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/structs"
)

// keyword describes sql keyword type.
type keyword string

const (
	// SelectKeyword returns SELECT keyword.
	SelectKeyword keyword = "SELECT"
	// FromKeyword returns FROM keyword.
	FromKeyword keyword = "FROM"
	// WhereKeyword returns WHERE keyword.
	WhereKeyword keyword = "WHERE"
	// AndKeyword returns AND keyword.
	AndKeyword keyword = "AND"
	// OrKeyword returns OR keyword.
	OrKeyword keyword = "OR"
)

// Column describes table column.
type Column string

// Columns describes list of table columns.
type Columns []Column

// Table describes table.
type Table string

// Tables describes list of tables.
type Tables []Table

// QueryColumns describes list of unique table columns.
type QueryColumns []Column

// add adds unique column to the list.
func (qc *QueryColumns) add(name Column) {
	for _, item := range *qc {
		if item == name {
			return
		}
	}

	*qc = append(*qc, name)
}

// A Query describes a query string.
type Query struct {
	query       string
	Select      Columns
	StarColumn  bool
	From        Tables
	Where       *structs.Tree
	UsedColumns QueryColumns
	cursor      int
	logger      *zap.Logger
}

var (
	// ErrIncorrectQuery return error if query string is incorrect.
	ErrIncorrectQuery = errors.New("incorrect query")
	// ErrIncorrectBracketPosition return error if query string has incorrect bracket positions in where statement.
	ErrIncorrectBracketPosition = fmt.Errorf("%w: incorrect bracket positions in where statement", ErrIncorrectQuery)
	// ErrTooManyStarColumns returns error if query string has more than one star in select statement.
	ErrTooManyStarColumns = fmt.Errorf("%w: too many star columns", ErrIncorrectQuery)
)

// NewQuery returns the query.
func NewQuery(query string, logger *zap.Logger) *Query {
	return &Query{
		query:  strings.TrimSpace(query),
		logger: logger,
	}
}

// Parse parses the sql like query string.
func (q *Query) Parse() error {
	err := q.ParseSelectStatement()
	if err != nil {
		return err
	}

	err = q.ParseFromStatement()
	if err != nil {
		return err
	}

	if q.cursor < len(q.query) {
		err = q.ParseWhereStatement()
		if err != nil {
			return err
		}
	}

	return nil
}

// ParseSelectStatement parses select statement.
func (q *Query) ParseSelectStatement() error {
	if !strings.HasPrefix(strings.ToUpper(q.query[q.cursor:]), string(SelectKeyword)+" ") {
		q.logger.Error("Not found SELECT statement")
		return ErrIncorrectQuery
	}
	q.cursor += len(SelectKeyword) + 1
	q.skipSpace()

	columns, err := q.parseSelectFromStatement()
	if err != nil {
		q.logger.Error("Incorrect select statement")
		return err
	}

	q.Select = make(Columns, len(columns))

	var countStarColumns int
	for i, column := range columns {
		q.Select[i] = Column(column)
		if column != "*" {
			q.UsedColumns.add(Column(column))
			continue
		}

		countStarColumns++
		q.StarColumn = true
	}

	if countStarColumns > 1 {
		return ErrTooManyStarColumns
	}

	return nil
}

// ParseFromStatement parses from statement.
func (q *Query) ParseFromStatement() error {
	if !strings.HasPrefix(strings.ToUpper(q.query[q.cursor:]), string(FromKeyword)+" ") {
		q.logger.Error("Not found FROM statement")
		return ErrIncorrectQuery
	}
	q.cursor += len(FromKeyword) + 1
	q.skipSpace()

	tables, err := q.parseSelectFromStatement()
	if err != nil {
		q.logger.Error("Incorrect FROM statement")
		return err
	}

	q.From = make(Tables, len(tables))
	for i, table := range tables {
		q.From[i] = Table(table)
	}

	return nil
}

// ParseWhereStatement parses where statement.
func (q *Query) ParseWhereStatement() error {
	if !strings.HasPrefix(strings.ToUpper(q.query[q.cursor:]), string(WhereKeyword)+" ") {
		q.logger.Error("Not found WHERE statement")
		return ErrIncorrectQuery
	}
	q.cursor += len(WhereKeyword) + 1
	q.skipSpace()

	parser := NewWhereParser(q.query[q.cursor:], q.logger)
	whereColumns, tree, err := parser.Parse()
	if err != nil {
		return err
	}
	q.Where = tree
	q.mergeColumns(whereColumns)

	return nil
}

func (q *Query) parseSelectFromStatement() ([]string, error) {
	var isCorrect = true
	var tokens []string

	for q.cursor < len(q.query) {
		firstSpace := strings.IndexRune(q.query[q.cursor:], ' ')
		firstComma := strings.IndexRune(q.query[q.cursor:], ',')

		if firstSpace == -1 && firstComma == -1 {
			token := q.query[q.cursor:]
			if token != "" {
				tokens = append(tokens, token)
			}
			q.cursor = len(q.query)
			break
		}

		// найдена только запятая
		if firstSpace == -1 {
			token := q.query[q.cursor : q.cursor+firstComma]
			q.cursor += firstComma + 1
			if token == "" || q.cursor >= len(q.query) {
				isCorrect = false
				break
			}

			tokens = append(tokens, token)
			continue
		}

		// найден только пробел
		if firstComma == -1 {
			// найден последний токен
			tokens = append(tokens, q.query[q.cursor:q.cursor+firstSpace])
			q.cursor += firstSpace + 1
			q.skipSpace()
			break
		}

		// найдена запятая и пробел (пробел раньше, между пробелом и запятой только пробелы)
		if firstComma > firstSpace {
			if strings.TrimSpace(q.query[q.cursor+firstSpace:q.cursor+firstComma]) == "" {
				tokens = append(tokens, q.query[q.cursor:q.cursor+firstSpace])
				q.cursor += firstComma + 1
				q.skipSpace()
				continue
			} else if strings.TrimSpace(q.query[q.cursor+firstSpace:q.cursor+firstComma]) != "" {
				tokens = append(tokens, q.query[q.cursor:q.cursor+firstSpace])
				q.cursor += firstSpace + 1
				q.skipSpace()
				break
			}
		}

		token := q.query[q.cursor : q.cursor+firstComma]
		if token == "" {
			isCorrect = false
			break
		}
		tokens = append(tokens, q.query[q.cursor:q.cursor+firstComma])
		q.cursor += firstComma + 1
		q.skipSpace()
	}

	if len(tokens) == 0 || !isCorrect {
		return nil, ErrIncorrectQuery
	}

	return tokens, nil
}

func (q *Query) mergeColumns(whereColumns map[Column]int) {
	for column := range whereColumns {
		q.UsedColumns.add(column)
	}
}

func (q *Query) skipSpace() {
	for _, char := range q.query[q.cursor:] {
		if char != ' ' {
			break
		}
		q.cursor++
	}
}

package csvquery

import (
	"errors"
	"strings"

	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/structs"
)

type keyword string

const (
	SelectKeyword keyword = "SELECT"
	FromKeyword   keyword = "FROM"
	WhereKeyword  keyword = "WHERE"
	AndKeyword    keyword = "AND"
	OrKeyword     keyword = "OR"
)

type Column string
type Columns []Column

type Table string
type Tables []Table

type Query struct {
	query       string
	Select      Columns
	From        Tables
	Where       *structs.Tree
	UsedColumns map[Column]int
	cursor      int
	logger      *zap.Logger
}

var (
	ErrIncorrectQuery           = errors.New("incorrect query")
	ErrIncorrectBracketPosition = errors.New("incorrect bracket positions in where statement")
)

func NewQuery(query string, logger *zap.Logger) *Query {
	return &Query{
		query:  strings.TrimSpace(query),
		logger: logger,
	}
}

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
	q.UsedColumns = make(map[Column]int)
	for i, column := range columns {
		q.Select[i] = Column(column)
		if column != "*" {
			q.UsedColumns[Column(column)] = 0
		}
	}

	return nil
}

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
		q.UsedColumns[column] = 0
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

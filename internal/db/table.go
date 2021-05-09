package db

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/phpCoder88/csv-searcher/internal/csvquery"
	"github.com/phpCoder88/csv-searcher/internal/structs"
)

// Table describes table entity.
type Table struct {
	name       csvquery.Table
	query      *csvquery.Query
	connection io.ReadCloser
	mapColumns map[csvquery.Column]int

	db *DB
}

// NewTable returns new instance of Table.
func NewTable(
	name csvquery.Table,
	query *csvquery.Query,
	db *DB,
) *Table {
	return &Table{
		name:       name,
		query:      query,
		mapColumns: make(map[csvquery.Column]int, len(query.UsedColumns)),
		db:         db,
	}
}

// Exists checks whether a table exists.
func (t *Table) Exists() bool {
	tablePath := path.Join(t.db.config.TableLocation, string(t.name))
	return t.db.connector.Exists(tablePath)
}

func (t *Table) executeOnTable(ctx context.Context) {
	reader, err := t.connect()
	if err != nil {
		t.db.logger.Error(err.Error())
		t.db.errorCh <- err
		return
	}

	defer func() {
		err = t.connection.Close()
		if err != nil {
			t.db.logger.Error(fmt.Errorf("%w: '%s', Real Error: %v", ErrTableDisconnection, t.name, err).Error())
		}
	}()

	tableColumnNames, err := reader.Read()
	if err != nil {
		userErr := fmt.Errorf("%w: '%s'", ErrTableColumnsRead, t.name)
		t.db.logger.Error(fmt.Errorf("%w, Real Error: %v", userErr, err).Error())
		t.db.errorCh <- userErr
		return
	}

	err = t.checkColumns(tableColumnNames)
	if err != nil {
		t.db.logger.Error(err.Error())
		t.db.errorCh <- err
		return
	}

	t.db.headersCh <- t.chooseColumns(&tableColumnNames)

	t.getRows(ctx, reader)
}

func (t *Table) connect() (*csv.Reader, error) {
	tablePath := path.Join(t.db.config.TableLocation, string(t.name))
	file, err := t.db.connector.GetReader(tablePath)
	if err != nil {
		// TODO оригинальная ошибка не используется
		err := fmt.Errorf("%w: %s", ErrTableConnection, t.name)
		return nil, err
	}

	t.connection = file

	reader := csv.NewReader(file)
	reader.Comma = t.db.config.FieldDelimiter

	return reader, nil
}

func (t *Table) checkColumns(tableColumns []string) error {
	queryColumnNames := make([]csvquery.Column, 0, len(t.query.UsedColumns))
	queryColumnNames = append(queryColumnNames, t.query.UsedColumns...)

	for i, colName := range tableColumns {
		for j, queryColName := range queryColumnNames {
			if csvquery.Column(colName) != queryColName {
				continue
			}

			t.mapColumns[queryColName] = i
			queryColumnNames = append(queryColumnNames[:j], queryColumnNames[j+1:]...)
			break
		}
	}

	if len(queryColumnNames) > 0 {
		return fmt.Errorf("%w: table: '%s', columns: %v", ErrNotExistColumn, t.name, queryColumnNames)
	}

	return nil
}

func (t *Table) getRows(ctx context.Context, reader *csv.Reader) {
	workerInput := make(chan []string, t.db.config.Workers)

	var wg sync.WaitGroup
	wg.Add(t.db.config.Workers)
	for i := 0; i < t.db.config.Workers; i++ {
		go func() {
			t.processRow(workerInput)
			wg.Done()
		}()
	}

reader:
	for atomic.LoadInt32(&t.db.selected) < t.db.config.Limit {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			t.db.logger.Error(err.Error())
			break
		}

		select {
		case <-ctx.Done():
			break reader
		case workerInput <- row:
			break
		}
	}

	close(workerInput)
	wg.Wait()
}

func (t *Table) processRow(in <-chan []string) {
	for input := range in {
		rowOk, err := t.checkRow(&input)
		if err != nil {
			t.db.errorCh <- err
			return
		}

		if rowOk {
			if atomic.LoadInt32(&t.db.selected) < t.db.config.Limit {
				atomic.AddInt32(&t.db.selected, 1)
				t.db.resultCh <- t.chooseColumns(&input)
			}
		}
		runtime.Gosched()
	}
}

func (t *Table) checkRow(columns *[]string) (bool, error) {
	if t.query.Where == nil {
		return true, nil
	}

	return t.calcConditions(t.query.Where, columns)
}

func (t *Table) chooseColumns(input *[]string) []string {
	filteredColumns := make([]string, 0, len(t.query.Select))

	for _, col := range t.query.Select {
		if col == "*" {
			filteredColumns = append(filteredColumns, *input...)
			continue
		}

		ind := t.mapColumns[col]
		filteredColumns = append(filteredColumns, (*input)[ind])
	}

	return filteredColumns
}

func (t *Table) calcConditions(node *structs.Tree, cols *[]string) (bool, error) {
	var leftRes bool
	var rightRes bool
	var err error

	if node.LeftChild() != nil {
		leftRes, err = t.calcConditions(node.LeftChild(), cols)
		if err != nil {
			return false, err
		}
	}

	op, ok := node.GetValue().(string)
	if ok && csvquery.IsOperator(op) {
		// Operator
		if leftRes && csvquery.IsSameOperator(op, csvquery.OrOperator) {
			return true, nil
		} else if !leftRes && csvquery.IsSameOperator(op, csvquery.AndOperator) {
			return false, nil
		}

		rightRes, err = t.calcConditions(node.RightChild(), cols)
		if err != nil {
			return false, err
		}

		return csvquery.Calc(leftRes, rightRes, csvquery.LogicalOperator(op)), nil
	}

	if node.RightChild() != nil {
		err = fmt.Errorf("%w: get not empty right tree branch", ErrIncorrectWhereTree)
		t.db.logger.Error(err.Error())
		return false, ErrIncorrectWhereTree
	}

	// Condition
	cond, ok := node.GetValue().(*csvquery.Condition)
	if !ok {
		err := fmt.Errorf("%w: cant cast cond %v to type *Condition", ErrIncorrectWhereTree, node.GetValue())
		t.db.logger.Error(err.Error())
		return false, ErrIncorrectWhereTree
	}

	return t.calcCondition(cond, cols)
}

func (t *Table) calcCondition(cond *csvquery.Condition, cols *[]string) (bool, error) {
	fieldInd := t.mapColumns[cond.Column]

	if fieldInd >= len(*cols) {
		err := fmt.Errorf("%w: there is not a column in row with ind %d, row: %v", ErrIncorrectTableRow, fieldInd, *cols)
		t.db.logger.Error(err.Error())
		return false, err
	}
	colValue := (*cols)[fieldInd]

	result, err := cond.CheckCondition(colValue)
	if err != nil {
		t.db.logger.Error(err.Error())
		return false, err
	}

	return result, nil
}

package db

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/phpCoder88/csv-searcher/internal/structs"

	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/config"
	"github.com/phpCoder88/csv-searcher/internal/csvquery"
)

var (
	ErrQueryTimeout         = errors.New("time for executing query is out")
	ErrNotExistColumn       = errors.New("column doesn't exist")
	ErrTableConnection      = errors.New("can't connect table")
	ErrTableDisconnection   = errors.New("can't disconnect table")
	ErrTableColumnsRead     = errors.New("can't read table columns")
	ErrIncorrectTableRow    = errors.New("incorrect table row")
	ErrIncorrectWhereTree   = errors.New("incorrect where tree")
	ErrIncorrectColumnOrder = errors.New("incorrect column order in tables")
	ErrIncorrectColumnCount = errors.New("incorrect column count")
)

func Execute(ctx context.Context, connector TableConnector, queryString string, conf *config.Config, logger *zap.Logger) error {
	logger.Info(fmt.Sprintf("Executing query: '%s'", queryString))
	logger = logger.With(zap.String("query", queryString))

	timeoutCtx, cancel := context.WithTimeout(ctx, conf.Timeout)
	defer cancel()

	query := csvquery.NewQuery(queryString, logger)
	db := NewDB(connector, query, logger, conf)

	go db.execute(timeoutCtx)

	var rows [][]string
done:
	for {
		select {
		case <-timeoutCtx.Done():
			t, _ := timeoutCtx.Deadline()
			if time.Since(t) >= 0 {
				logger.Error(fmt.Sprintf("Timeout %s", conf.Timeout))
			}
			<-db.finishedCh
			return ErrQueryTimeout

		case <-db.finishedCh:
			break done

		case err := <-db.errorCh:
			return err

		case header := <-db.headersCh:
			if rows == nil {
				rows = append(rows, header)
				continue
			}

			err := db.checkTableColumnNames(rows[0], header)
			if err != nil {
				return err
			}

		case row := <-db.resultCh:
			rows = append(rows, row)
		}
	}

	close(db.resultCh)
	close(db.headersCh)
	close(db.finishedCh)

	return db.printResult(rows)
}

type DB struct {
	connector TableConnector
	query     *csvquery.Query
	logger    *zap.Logger
	config    *config.Config

	sync.Mutex
	connections map[csvquery.Table]io.ReadCloser

	mapLock          sync.RWMutex
	mapTable2Columns map[csvquery.Table]map[csvquery.Column]int

	selected   int32
	errorCh    chan error
	finishedCh chan struct{}
	resultCh   chan []string
	headersCh  chan []string
}

func NewDB(connector TableConnector, query *csvquery.Query, logger *zap.Logger, conf *config.Config) *DB {
	return &DB{
		connector:   connector,
		query:       query,
		finishedCh:  make(chan struct{}),
		errorCh:     make(chan error),
		logger:      logger,
		config:      conf,
		connections: make(map[csvquery.Table]io.ReadCloser),
		resultCh:    make(chan []string, conf.Limit),
		headersCh:   make(chan []string),
	}
}

func (db *DB) checkTableColumnNames(currentHeaders, headers []string) error {
	if !db.query.StarColumn {
		return nil
	}

	if len(currentHeaders) != len(headers) {
		db.logger.Error(fmt.Errorf("%w: expected: %v, actual: %v", ErrIncorrectColumnCount, currentHeaders, headers).Error())
		return ErrIncorrectColumnCount
	}

	for i, item := range currentHeaders {
		if item != headers[i] {
			db.logger.Error(fmt.Errorf("%w: expected: %v, actual: %v", ErrIncorrectColumnOrder, currentHeaders, headers).Error())
			return ErrIncorrectColumnOrder
		}
	}

	return nil
}

func (db *DB) printResult(rows [][]string) error {
	var err error
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)

loop:
	for _, row := range rows {
		for _, item := range row {
			_, err = fmt.Fprintf(w, "%s\t", item)
			if err != nil {
				break loop
			}
		}
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			break loop
		}
	}

	if err != nil {
		db.logger.Error(err.Error())
		return err
	}

	err = w.Flush()
	if err != nil {
		db.logger.Error(err.Error())
		return err
	}

	return nil
}

func (db *DB) execute(ctx context.Context) {
	defer func() {
		db.finishedCh <- struct{}{}
	}()

	err := db.query.Parse()
	if err != nil {
		db.errorCh <- err
		return
	}

	db.mapTable2Columns = make(map[csvquery.Table]map[csvquery.Column]int, len(db.query.From))

	db.executeQuery(ctx)
}

func (db *DB) executeQuery(ctx context.Context) {
	var wg sync.WaitGroup

	for _, table := range db.query.From {
		if !db.tableExist(table) {
			err := fmt.Errorf("%w: table '%s' doen't exist", csvquery.ErrIncorrectQuery, table)
			db.logger.Error(err.Error())
			db.errorCh <- err

			return
		}

		wg.Add(1)
		go db.executeOnTable(ctx, table, &wg)
	}

	wg.Wait()
}

func (db *DB) tableExist(table csvquery.Table) bool {
	tablePath := path.Join(db.config.TableLocation, string(table))
	return db.connector.Exists(tablePath)
}

func (db *DB) executeOnTable(ctx context.Context, table csvquery.Table, wg *sync.WaitGroup) {
	defer wg.Done()

	reader, err := db.connectTable(table)
	if err != nil {
		db.logger.Error(err.Error())
		db.errorCh <- err
		return
	}

	defer func() {
		db.Lock()
		// TODO предусмотреть возможность использовать одну таблицу дважды в FROM с корректным закрытием
		// (на второй инстанс выдает ошибку об уже закрытом файле)
		err = db.connections[table].Close()
		db.Unlock()
		if err != nil {
			db.logger.Error(fmt.Errorf("%w: '%s', Real Error: %v", ErrTableDisconnection, table, err).Error())
		}
	}()

	tableColumnNames, err := reader.Read()
	if err != nil {
		userErr := fmt.Errorf("%w: '%s'", ErrTableColumnsRead, table)
		db.logger.Error(fmt.Errorf("%w, Real Error: %v", userErr, err).Error())
		db.errorCh <- userErr
		return
	}

	err = db.checkTableColumns(table, tableColumnNames)
	if err != nil {
		db.logger.Error(err.Error())
		db.errorCh <- err
		return
	}

	db.headersCh <- db.chooseColumns(&tableColumnNames, table)

	db.getRows(ctx, reader, table)
}

func (db *DB) connectTable(table csvquery.Table) (*csv.Reader, error) {
	tablePath := path.Join(db.config.TableLocation, string(table))
	file, err := db.connector.GetReader(tablePath)
	if err != nil {
		err := fmt.Errorf("%w: %s", ErrTableConnection, table)
		return nil, err
	}

	db.Lock()
	db.connections[table] = file
	db.Unlock()

	reader := csv.NewReader(file)
	reader.Comma = db.config.FieldDelimiter
	return reader, nil
}

func (db *DB) checkTableColumns(table csvquery.Table, tableColumns []string) error {
	queryColumnNames := make([]csvquery.Column, 0, len(db.query.UsedColumns))
	queryColumnNames = append(queryColumnNames, db.query.UsedColumns...)
	mapTableColumns := make(map[csvquery.Column]int, len(db.query.UsedColumns))

	for i, colName := range tableColumns {
		for j, queryColName := range queryColumnNames {
			if csvquery.Column(colName) != queryColName {
				continue
			}

			mapTableColumns[queryColName] = i
			queryColumnNames = append(queryColumnNames[:j], queryColumnNames[j+1:]...)
			break
		}
	}

	if len(queryColumnNames) > 0 {
		return fmt.Errorf("%w: table: '%s', columns: %v", ErrNotExistColumn, table, queryColumnNames)
	}

	db.mapLock.Lock()
	db.mapTable2Columns[table] = mapTableColumns
	db.mapLock.Unlock()

	return nil
}

func (db *DB) getRows(ctx context.Context, reader *csv.Reader, table csvquery.Table) {
	workerInput := make(chan []string, db.config.Workers)

	var wg sync.WaitGroup
	wg.Add(db.config.Workers)
	for i := 0; i < db.config.Workers; i++ {
		go db.processRow(workerInput, &wg, table)
	}

reader:
	for atomic.LoadInt32(&db.selected) < db.config.Limit {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			db.logger.Error(err.Error())
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

func (db *DB) processRow(in <-chan []string, wg *sync.WaitGroup, table csvquery.Table) {
	defer wg.Done()

	for input := range in {
		rowOk, err := db.checkRow(&input, table)
		if err != nil {
			db.errorCh <- err
			return
		}

		if rowOk {
			if atomic.LoadInt32(&db.selected) < db.config.Limit {
				atomic.AddInt32(&db.selected, 1)
				db.resultCh <- db.chooseColumns(&input, table)
			}
		}
		runtime.Gosched()
	}
}

func (db *DB) checkRow(columns *[]string, table csvquery.Table) (bool, error) {
	if db.query.Where == nil {
		return true, nil
	}

	return db.calcConditions(table, db.query.Where, columns)
}

func (db *DB) chooseColumns(input *[]string, table csvquery.Table) []string {
	filteredColumns := make([]string, 0, len(db.query.Select))

	for _, col := range db.query.Select {
		if col == "*" {
			filteredColumns = append(filteredColumns, *input...)
			continue
		}

		db.mapLock.RLock()
		ind := db.mapTable2Columns[table][col]
		db.mapLock.RUnlock()

		filteredColumns = append(filteredColumns, (*input)[ind])
	}

	return filteredColumns
}

func (db *DB) calcConditions(table csvquery.Table, node *structs.Tree, cols *[]string) (bool, error) {
	var leftRes bool
	var rightRes bool
	var err error

	if node.LeftChild() != nil {
		leftRes, err = db.calcConditions(table, node.LeftChild(), cols)
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

		rightRes, err = db.calcConditions(table, node.RightChild(), cols)
		if err != nil {
			return false, err
		}

		return csvquery.Calc(leftRes, rightRes, csvquery.LogicalOperator(op)), nil
	}

	if node.RightChild() != nil {
		err = fmt.Errorf("%w: get not empty right tree branch", ErrIncorrectWhereTree)
		db.logger.Error(err.Error())
		return false, ErrIncorrectWhereTree
	}

	// Condition
	cond, ok := node.GetValue().(*csvquery.Condition)
	if !ok {
		err := fmt.Errorf("%w: cant cast cond %v to type *Condition", ErrIncorrectWhereTree, node.GetValue())
		db.logger.Error(err.Error())
		return false, ErrIncorrectWhereTree
	}

	return db.calcCondition(table, cond, cols)
}

func (db *DB) calcCondition(table csvquery.Table, cond *csvquery.Condition, cols *[]string) (bool, error) {
	db.mapLock.RLock()
	fieldInd := db.mapTable2Columns[table][cond.Column]
	db.mapLock.RUnlock()

	if fieldInd >= len(*cols) {
		err := fmt.Errorf("%w: there is not a column in row with ind %d, row: %v", ErrIncorrectTableRow, fieldInd, *cols)
		db.logger.Error(err.Error())
		return false, err
	}
	colValue := (*cols)[fieldInd]

	result, err := cond.CheckCondition(colValue)
	if err != nil {
		db.logger.Error(err.Error())
		return false, err
	}

	return result, nil
}

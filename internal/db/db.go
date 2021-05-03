package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/config"
	"github.com/phpCoder88/csv-searcher/internal/csvquery"
)

var (
	// ErrQueryTimeout indicates that time for executing query is out.
	ErrQueryTimeout = errors.New("time for executing query is out")
	// ErrNotExistColumn indicates that given column doesn't exist.
	ErrNotExistColumn = errors.New("column doesn't exist")
	// ErrTableConnection indicates that executor can't connect to the given table.
	ErrTableConnection = errors.New("can't connect table")
	// ErrTableDisconnection indicates that executor can't disconnect the table.
	ErrTableDisconnection = errors.New("can't disconnect table")
	// ErrTableColumnsRead indicates that executor can't read the table columns.
	ErrTableColumnsRead = errors.New("can't read table columns")
	// ErrIncorrectTableRow indicates that executor got incorrect table row.
	ErrIncorrectTableRow = errors.New("incorrect table row")
	// ErrIncorrectWhereTree indicates that executor got incorrect where tree.
	ErrIncorrectWhereTree = errors.New("incorrect where tree")
	// ErrIncorrectColumnOrder indicates that executor got incorrect column order in tables.
	ErrIncorrectColumnOrder = errors.New("incorrect column order in tables")
	// ErrIncorrectColumnCount indicates that executor got incorrect column count.
	ErrIncorrectColumnCount = errors.New("incorrect column count")
)

// Execute executes query.
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

// DB describes file database.
type DB struct {
	connector TableConnector
	query     *csvquery.Query
	logger    *zap.Logger
	config    *config.Config

	selected   int32
	errorCh    chan error
	finishedCh chan struct{}
	resultCh   chan []string
	headersCh  chan []string
}

// NewDB returns new instance of DB.
func NewDB(connector TableConnector, query *csvquery.Query, logger *zap.Logger, conf *config.Config) *DB {
	return &DB{
		connector:  connector,
		query:      query,
		finishedCh: make(chan struct{}),
		errorCh:    make(chan error),
		logger:     logger,
		config:     conf,
		resultCh:   make(chan []string, conf.Limit),
		headersCh:  make(chan []string),
	}
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

	db.executeQuery(ctx)
}

func (db *DB) executeQuery(ctx context.Context) {
	var wg sync.WaitGroup

	for _, tableName := range db.query.From {
		table := NewTable(tableName, db.query, db)
		if !table.Exists() {
			err := fmt.Errorf("%w: table '%s' doen't exist", csvquery.ErrIncorrectQuery, tableName)
			db.logger.Error(err.Error())
			db.errorCh <- err

			return
		}

		wg.Add(1)
		go table.executeOnTable(ctx, &wg)
	}

	wg.Wait()
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

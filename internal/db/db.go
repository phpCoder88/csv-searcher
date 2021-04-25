package db

import (
	"context"
	"fmt"

	"github.com/phpCoder88/csv-searcher/internal/config"
	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/csvquery"
)

func Execute(ctx context.Context, queryString string, conf *config.Config, logger *zap.Logger) error {
	logger.Info(fmt.Sprintf("Executing query: '%s'", queryString))
	logger = logger.With(zap.String("query", queryString))

	timeoutCtx, cancel := context.WithTimeout(ctx, conf.Timeout)
	defer cancel()

	query := csvquery.NewQuery(queryString, logger)
	db := NewDB(query)

	defer close(db.resultCh)

	go db.Execute(timeoutCtx)

	select {
	case <-timeoutCtx.Done():
		logger.Error(fmt.Sprintf("Timeout %s", conf.Timeout))
		return nil

	case <-db.resultCh:
		return nil
	}
}

type DB struct {
	query    *csvquery.Query
	resultCh chan struct{}
}

func NewDB(query *csvquery.Query) *DB {
	return &DB{
		query:    query,
		resultCh: make(chan struct{}),
	}
}

func (db *DB) Execute(ctx context.Context) {
	defer func() {
		db.resultCh <- struct{}{}
	}()
	err := db.query.Parse()
	if err != nil {
		return
	}
}

func (db *DB) Connect() error {
	return nil
}

/*func (db *DB) tableConnect() error {
	return nil
}*/

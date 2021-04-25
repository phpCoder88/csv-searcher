package sqlreader

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
)

type Reader struct {
	bufioReader *bufio.Reader
}

var (
	ErrInterrupted = errors.New("context canceled")
)

func NewSQLReader(rd io.Reader) *Reader {
	reader := bufio.NewReader(rd)
	return &Reader{
		bufioReader: reader,
	}
}

func (r *Reader) ReadLine(ctx context.Context) (string, error) {
	var resultStr string
	var resultErr error

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		resultStr, resultErr = r.bufioReader.ReadString('\n')
	}()

	select {
	case <-ctx.Done():
		return "", ErrInterrupted
	case <-doneCh:
		return strings.TrimSpace(resultStr), resultErr
	}
}

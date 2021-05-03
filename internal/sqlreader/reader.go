// Package sqlreader reads lines with with the ability to cancel the context.
package sqlreader

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
)

// Reader describes reader
type Reader struct {
	bufioReader *bufio.Reader
}

// ErrInterrupted error for canceled context.
var ErrInterrupted = errors.New("context canceled")

// NewSQLReader returns a new Reader
func NewSQLReader(rd io.Reader) *Reader {
	reader := bufio.NewReader(rd)
	return &Reader{
		bufioReader: reader,
	}
}

// ReadLine returns the read string or error.
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

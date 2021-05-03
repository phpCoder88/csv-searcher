package db

import (
	"io"
	"os"
)

// TableConnector is the interface that groups the basic GetReader and Exists methods.
type TableConnector interface {
	GetReader(string) (io.ReadCloser, error)
	Exists(string) bool
}

// FileTableConnector implements TableConnector interface for working with files.
type FileTableConnector struct{}

// GetReader returns file reader.
func (c FileTableConnector) GetReader(tablePath string) (io.ReadCloser, error) {
	file, err := os.Open(tablePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Exists checks whether a file exists.
func (c FileTableConnector) Exists(tablePath string) bool {
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return false
	}
	return true
}

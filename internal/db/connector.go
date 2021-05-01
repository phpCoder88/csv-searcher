package db

import (
	"io"
	"os"
)

type TableConnector interface {
	GetReader(string) (io.ReadCloser, error)
	Exists(string) bool
}

type FileTableConnector struct{}

func (c FileTableConnector) GetReader(tablePath string) (io.ReadCloser, error) {
	file, err := os.Open(tablePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (c FileTableConnector) Exists(tablePath string) bool {
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		return false
	}
	return true
}

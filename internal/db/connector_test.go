package db

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TableConnectorTestSuite struct {
	suite.Suite
	conn     TableConnector
	filename string
}

func (s *TableConnectorTestSuite) SetupTest() {
	s.conn = FileTableConnector{}
	s.filename = "users.csv"

	file, err := os.Create(s.filename)
	if err != nil {
		s.T().Fatal(err)
	}
	_ = file.Close()
}
func (s *TableConnectorTestSuite) TearDownTest() {
	_ = os.Remove(s.filename)
}

func (s *TableConnectorTestSuite) TestFileTableConnector_Exists() {
	assert.True(s.T(), s.conn.Exists(s.filename))
	_ = os.Remove(s.filename)
	assert.False(s.T(), s.conn.Exists(s.filename))
}

func (s *TableConnectorTestSuite) TestFileTableConnector_GetReader() {
	reader, err := s.conn.GetReader(s.filename)
	assert.NoError(s.T(), err)
	assert.Implements(s.T(), (*io.ReadCloser)(nil), reader)

	_ = os.Remove(s.filename)
	reader, err = s.conn.GetReader(s.filename)
	assert.Nil(s.T(), reader)
	assert.Error(s.T(), err)
}

func TestTableConnectorTestSuite(t *testing.T) {
	suite.Run(t, new(TableConnectorTestSuite))
}

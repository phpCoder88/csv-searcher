package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetConfig_default(t *testing.T) {
	conf, err := GetConfig()
	expectedConf := Config{
		Timeout:        500000000,
		Workers:        50,
		TableLocation:  "./",
		Limit:          100,
		Delimiter:      ",",
		FieldDelimiter: ',',
	}
	assert.NoError(t, err)
	assert.Equal(t, expectedConf, *conf)
}

func TestGetConfig_DelimiterError(t *testing.T) {
	_ = os.Setenv("DELIMITER", ",,")
	conf, err := GetConfig()
	assert.Equal(t, ErrIncorrectDelimiter, err)
	assert.Nil(t, conf)
	_ = os.Unsetenv("DELIMITER")
}

func TestGetConfig_EnvFile(t *testing.T) {
	file, err := os.Create(".env")
	if err != nil {
		t.Fatal(err)
	}

	envFileContent := `TIMEOUT="5000ms"
TABLELOCATION="./testdata"
WORKERS=100
LIMIT=1000
DELIMITER=";"`

	_, _ = file.WriteString(envFileContent)
	_ = file.Close()

	conf, err := GetConfig()
	expectedConf := Config{
		Timeout:        5000000000,
		Workers:        100,
		TableLocation:  "./testdata",
		Limit:          1000,
		Delimiter:      ";",
		FieldDelimiter: ';',
	}
	assert.NoError(t, err)
	assert.Equal(t, expectedConf, *conf)

	_ = os.Remove(".env")
}

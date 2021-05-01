package config

import (
	"errors"
	"os"
	"time"
	"unicode/utf8"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Timeout        time.Duration `default:"500ms"`
	Workers        int           `default:"50"`
	TableLocation  string        `default:"./"`
	Limit          int32         `default:"100"`
	Delimiter      string        `default:","`
	FieldDelimiter rune
}

var ErrIncorrectDelimiter = errors.New("incorrect delimiter. there must be only one rune in string")

func GetConfig() (*Config, error) {
	var err error

	if _, err = os.Stat(".env"); !os.IsNotExist(err) {
		err = godotenv.Load()
		if err != nil {
			return nil, err
		}
	}

	var conf Config
	err = envconfig.Process("", &conf)
	if err != nil {
		return nil, err
	}

	if utf8.RuneCountInString(conf.Delimiter) != 1 {
		return nil, ErrIncorrectDelimiter
	}

	conf.FieldDelimiter, _ = utf8.DecodeRuneInString(conf.Delimiter)
	return &conf, nil
}

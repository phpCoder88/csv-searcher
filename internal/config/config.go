package config

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Timeout       time.Duration `default:"500ms"`
	Workers       int           `default:"50"`
	TableLocation string        `default:"./"`
	Limit         int32         `default:"100"`
}

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

	return &conf, nil
}

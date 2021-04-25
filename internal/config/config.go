package config

import (
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Timeout time.Duration `default:"2s"`
}

func GetConfig() (*Config, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, err
	}
	var conf Config
	err = envconfig.Process("", &conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

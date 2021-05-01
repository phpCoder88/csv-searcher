package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/phpCoder88/csv-searcher/internal/db"
	"github.com/phpCoder88/csv-searcher/internal/sqlreader"

	"github.com/natefinch/lumberjack"
	"github.com/phpCoder88/csv-searcher/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type App struct {
	conf   *config.Config
	logger *zap.Logger
}

func NewApp() *App {
	logger := createLogger()

	conf, err := config.GetConfig()
	if err != nil {
		logger.Fatal(err.Error())
		return nil
	}

	return &App{
		logger: logger,
		conf:   conf,
	}
}

func (app *App) Run() {
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			fmt.Println(err)
		}
	}(app.logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go app.waitSignal(cancel)

	appDir, err := os.Getwd()
	if err != nil {
		app.logger.Fatal(err.Error())
		return
	}

	fmt.Println("Welcome to the CsvDB monitor.")
	fmt.Printf("Your location is %s\n\n", appDir)
	fmt.Printf("Copyright (c) 2021 Bobylev Pavel\n\n")

	reader := sqlreader.NewSQLReader(os.Stdin)
	for {
		fmt.Printf("CsvDB > ")

		input, err := reader.ReadLine(ctx)
		if err != nil {
			if err == io.EOF {
				app.logger.Error("End of line")
				fmt.Println("")
			}
			break
		}

		if strings.EqualFold("exit", input) {
			app.logger.Info("Exiting...")
			break
		}

		if input == "" {
			continue
		}

		err = db.Execute(ctx, db.FileTableConnector{}, input, app.conf, app.logger)
		if err != nil {
			app.logger.Error(err.Error())
			_, _ = fmt.Fprintln(os.Stderr, "ERROR:", err)
		}
	}
}

func (app *App) waitSignal(cancel context.CancelFunc) {
	osSignalChan := make(chan os.Signal, 1)
	defer signal.Stop(osSignalChan)
	signal.Notify(osSignalChan, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	sig := <-osSignalChan
	app.logger.Error(fmt.Sprintf("got signal %q", sig.String()))
	cancel()
}

func createLogger() *zap.Logger {
	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})

	errorLumberJackLogger := &lumberjack.Logger{
		Filename:   "./error.log",
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
	}
	accessLumberJackLogger := &lumberjack.Logger{
		Filename:   "./access.log",
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   false,
	}

	consoleDebugging := zapcore.AddSync(accessLumberJackLogger)
	consoleErrors := zapcore.AddSync(errorLumberJackLogger)

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, consoleErrors, highPriority),
		zapcore.NewCore(encoder, consoleDebugging, lowPriority),
	)
	return zap.New(core, zap.AddCaller())
}

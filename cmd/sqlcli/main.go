// Package main is console app for searching in csv files with sql like syntax.
package main

import (
	"github.com/phpCoder88/csv-searcher/internal/app"
)

func main() {
	appInst := app.NewApp()
	appInst.Run()
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/phpCoder88/csv-searcher/internal/csvquery"
)

func main() {
	appDir, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Welcome to the CsvDB monitor.")
	fmt.Printf("Your location is %s\n\n", appDir)
	fmt.Printf("Copyright (c) 2021 Bobylev Pavel\n\n")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("CsvDB > ")
		queryInput, _ := reader.ReadString('\n')
		fmt.Println(queryInput)

		query := csvquery.NewQuery(strings.TrimSpace(queryInput))
		err := query.Parse()
		if err != nil {
			fmt.Println(err)
		}
	}
}

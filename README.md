[![Go Report Card](https://goreportcard.com/badge/github.com/phpCoder88/csv-searcher)](https://goreportcard.com/report/github.com/phpCoder88/csv-searcher)
[![codecov](https://codecov.io/gh/phpCoder88/csv-searcher/branch/master/graph/badge.svg)](https://codecov.io/gh/phpCoder88/csv-searcher)

# Курсовая по курсу Лучшие практики разработки Go-приложений

## Query syntax

**SELECT**
    *select_expr* [, *select_expr* ] ...
**FROM**
    *table_references* [, *table_references* ] ...
[ **WHERE** *where_condition* ]

The most commonly used clauses of SELECT statements are these:

- Each ***select_expr*** indicates a column that you want to retrieve. There must be at least one ***select_expr***.
- A select list consisting only of a single unqualified * can be used as shorthand to select all columns from tables, but all tables must have the same columns and column order
- ***table_references*** indicates the table or tables from which to retrieve rows
- The WHERE clause, if given, indicates the condition or conditions that rows must satisfy to be selected. ***where_condition*** is an expression that evaluates to true for each row to be selected. The statement selects all rows if there is no WHERE clause.

build:
	go build ./...

test:
	go test -coverprofile=profile.out ./...

test-cover: test
	go tool cover -html=profile.out

check:
	golangci-lint run

run:
	go run -race cmd/sqlcli/main.go
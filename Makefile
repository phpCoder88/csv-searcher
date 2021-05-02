APP = sqlcli
BUILD_DIR = build
REPO = $(shell go list -m)
BUILD_DATE = $(shell date +%FT%T%Z)
BUILD_COMMIT = $(shell git rev-parse HEAD)
VERSION = $(if $(TAG),$(TAG),$(if $(BRANCH_NAME),$(BRANCH_NAME),$(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)))

GO_ASMFLAGS = -asmflags "all=-trimpath=$(shell dirname $(PWD))"
GO_GCFLAGS = -gcflags "all=-trimpath=$(shell dirname $(PWD))"
GO_BUILD_ARGS = \
  -ldflags " \
    -X '$(REPO)/internal/version.Version=$(VERSION)' \
    -X '$(REPO)/internal/version.BuildCommit=$(BUILD_COMMIT)' \
    -X '$(REPO)/internal/version.BuildDate=$(BUILD_DATE)' \
  " \

build:
	@echo "+ $@"
	@mkdir -p $(BUILD_DIR)
	go build -race $(GO_BUILD_ARGS) -o $(BUILD_DIR) ./cmd/sqlcli

test:
	@echo "+ $@"
	go test -cover ./...

test-cover:
	@echo "+ $@"
	go test -coverprofile=profile.out ./...
	go tool cover -html=profile.out
	rm profile.out

check:
	golangci-lint run

run: clean build
	@echo "+ $@"
	./${BUILD_DIR}/${APP}

clean:
	@rm -rf $(BUILD_DIR)
LANGCI := $(shell command -v golangci-lint 2> /dev/null) 

## help: print this help message.
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## test: execute all unit tests.
.PHONY: test
test:
	go test -v -race -buildvcs ./... -timeout 5s

## benchmark: execute all bench marks.
.PHONY: benchmark
	go test -bench=./...

## cover: execute all unit tests with coverage.
.PHONY: cover
cover:
	go test -v -race -buildvcs -coverprofile=/tmp/coverage.out ./...
	go tool cover -html=/tmp/coverage.out

## audit: audit the source code.
.PHONY: audit
audit: test
	go mod tidy --diff
	go mod verify
	test -z "$(shell gofmt -l .)"
	go vet ./...
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
	ifndef LANGCI
		@echo "golangci-lint not installed, please install it on your system before continuing, skipping"
	else
		golangci-lint run
	endif


## tidy: tidy modfiles and format
tidy:
	go mod tidy -v
	go fmt ./...
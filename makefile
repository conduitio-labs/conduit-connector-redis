.PHONY: build test lint clean

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-redis.version=${VERSION}'" -o conduit-connector-redis cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.20

clean:
	golangci-lint cache clean
	go clean -testcache
	rm conduit-connector-redis

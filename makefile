.PHONY: build test lint clean

build:
	go build -o conduit-connector-redis cmd/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run -c .golangci.yml --go=1.18

clean:
	golangci-lint cache clean
	go clean -testcache
	rm conduit-connector-redis
VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-redis.version=${VERSION}'" -o conduit-connector-redis cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: clean
clean:
	golangci-lint cache clean
	go clean -testcache
	rm conduit-connector-redis

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

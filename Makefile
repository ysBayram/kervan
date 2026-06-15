BINARY := bin/kervan
MODULE := github.com/ysBayram/kervan

.PHONY: all build test bench lint clean fmt

all: build

build:
	go build -o $(BINARY) ./cmd/kervan

test:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

clean:
	rm -rf bin/

bench-gate:
	go test -bench=BenchmarkRelay -benchmem -count=5 ./internal/proxy/
	go test -bench=BenchmarkSessionManagerGet -benchmem -count=5 ./internal/session/

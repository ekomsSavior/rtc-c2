.PHONY: all clean build build-linux build-windows test run-operator run-beacon deps

# Default
all: build

# Build all targets
build: build-linux build-windows

build-linux:
	@echo "Building Linux binaries..."
	GOOS=linux GOARCH=amd64 go build -o bin/beacon ./cmd/beacon/
	GOOS=linux GOARCH=amd64 go build -o bin/operator ./cmd/operator/

build-windows:
	@echo "Building Windows binaries..."
	GOOS=windows GOARCH=amd64 go build -o bin/beacon.exe ./cmd/beacon/
	GOOS=windows GOARCH=amd64 go build -o bin/operator.exe ./cmd/operator/

# Cross-compile
build-arm64:
	GOOS=linux GOARCH=arm64 go build -o bin/beacon-arm64 ./cmd/beacon/
	GOOS=linux GOARCH=arm64 go build -o bin/operator-arm64 ./cmd/operator/

build-darwin:
	GOOS=darwin GOARCH=amd64 go build -o bin/beacon-darwin ./cmd/beacon/
	GOOS=darwin GOARCH=amd64 go build -o bin/operator-darwin ./cmd/operator/

# Quick dev test
test:
	@echo "Running tests..."
	go test ./...

# Run in two terminals
run-operator:
	./bin/operator -sig-addr :9090 -session rtc-c2-test

run-beacon:
	./bin/beacon -signaller http://127.0.0.1:9090 -session rtc-c2-test

# Clean artifacts
clean:
	rm -rf bin/

# Dependencies
deps:
	go mod tidy
	go mod verify

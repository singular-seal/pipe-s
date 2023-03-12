.PHONY: build test clean build-linux
default: build

BINARY_PATH = "bin"

build:
	go build -o ${BINARY_PATH}/task cmd/task/main.go

build-linux:
	GOOS=linux GOARCH=amd64 go build -o ${BINARY_PATH}/task cmd/task/main.go

test:
	go test ./...

clean:
	rm -rf ${BINARY_PATH}


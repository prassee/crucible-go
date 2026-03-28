fmt:
    gofmt -w src/

default: run

build:
    go build -o bin/crucible ./src

run: build
    ./bin/crucible

clean:
    rm -rf bin/

go-mod:
    go mod tidy

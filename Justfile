default: run

build:
    go build -o bin/crucible src/main.go

run: build
    ./bin/crucible

clean:
    rm -rf bin/

mod:
    go mod tidy

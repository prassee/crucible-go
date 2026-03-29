set shell := ["fish", "-c"]

default: run
go-mod:
    go mod tidy

clean:
    rm -rf bin/

fmt: go-mod
    gofmt -w .

build: fmt clean
    go build -o bin/crucible ./src

docker-build: build
    docker build -t crucible:latest .

k8s-push docker_name='desktop': docker-build
    kind load docker-image crucible:latest --name {{docker_name}}

k8s-deploy: k8s-push
    kubectl apply -f k8s-job.yaml

run: build
    ./bin/crucible

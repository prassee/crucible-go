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

k8s-scaledown:
    kubectl delete job crucible-job --namespace lakehouse || true

# k8s-push
k8s-deploy: k8s-scaledown
    kubectl apply -f crucible-config-cm.yaml --namespace lakehouse
    kubectl apply -f k8s-job.yaml --namespace lakehouse

run: build
    ./bin/crucible

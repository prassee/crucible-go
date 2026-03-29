# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o crucible ./src

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/crucible ./crucible
COPY config.yaml ./config.yaml
ENTRYPOINT ["/app/crucible"]

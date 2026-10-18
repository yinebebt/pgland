.PHONY: run worker build test

VERSION ?= 0.1.0

ifneq (,$(wildcard .env))
include .env
export
endif

run:
	go run ./cmd/pgland

worker:
	go run ./cmd/pgland worker

build:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/pgland ./cmd/pgland

# Store tests skip unless DATABASE_URL is set.
test:
	go test -race ./...


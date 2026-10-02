.PHONY: run build test tidy db-up db-down

VERSION ?= 0.1.0

ifneq (,$(wildcard .env))
include .env
export
endif

run:
	go run ./cmd/pgland

build:
	go build -ldflags "-X main.version=$(VERSION)" -o bin/pgland ./cmd/pgland

# Store tests skip unless DATABASE_URL is set.
test:
	go test -race ./...

tidy:
	go mod tidy

db-up:
	docker compose up -d --wait

# Keeps the data volume.
db-down:
	docker compose down

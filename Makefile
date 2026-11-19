.PHONY: all build test race bench lint run-cluster docker-up docker-down clean

BIN_DIR := bin
GO := go

all: lint test build

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(BIN_DIR)/minikv-node ./cmd/minikv-node
	$(GO) build -trimpath -o $(BIN_DIR)/minikv-coord ./cmd/minikv-coord
	$(GO) build -trimpath -o $(BIN_DIR)/loadgen ./cmd/loadgen

test:
	$(GO) test -v -cover -count=1 ./...

race:
	$(GO) test -race -v -count=1 ./...

bench:
	$(GO) test -bench=. -benchmem -run=^# ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "Unformatted files found:" && gofmt -l . && exit 1)
	$(GO) vet ./...

run-cluster:
	./scripts/run-cluster.sh

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -rf $(BIN_DIR) data/ *.log *.snap *.tmp *.out *.exe bench-*.json bench-*.md

.PHONY: build run test test-race e2e bench lint docker clean

build:
	go build -o bin/continuum ./cmd/continuum

run:
	go run ./cmd/continuum

test:
	go test -v ./...

e2e:
	go test -v ./api/... -run E2E

bench:
	go test -bench=. -benchmem ./benchmarks/

lint:
	golangci-lint run ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

docker:
	docker build -t continuum .

docker-run:
	docker compose up

clean:
	rm -rf bin/ coverage.out

grafana:
	open http://localhost:3000

prometheus:
	open http://localhost:9090

metrics:
	open http://localhost:8080/metrics
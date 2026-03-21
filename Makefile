.PHONY: build run test bench lint docker clean

build:
	go build -o bin/continuum ./cmd/continuum

run:
	go run ./cmd/continuum

test:
	go test -v -race ./...

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
	docker-compose up

clean:
	rm -rf bin/ coverage.out
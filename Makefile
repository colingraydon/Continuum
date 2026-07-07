.PHONY: build run test test-race e2e e2e-integration fault sim sim-race bench bench-ci bench-report lint docker clean

build:
	go build -o bin/continuum ./cmd/continuum

run:
	go run ./cmd/continuum

test:
	go test -v -coverprofile=coverage.out -covermode=atomic ./...

test-race:
	go test -race ./...

e2e:
	go test -v ./api/... -run E2E

e2e-integration:
	go test -v -tags e2e -timeout 120s ./tests/e2e/...

fault:
	go test -v -tags fault -timeout 900s ./tests/fault/...

# Seeded in-process cluster simulation. SIM_SEEDS=n sweeps n seeds per
# scenario (default 3); SIM_SEED=k replays one seed; SIM_LOG=1 keeps node logs.
sim:
	go test -v -tags sim -timeout 600s ./tests/sim/...

# The whole cluster shares one process here, so -race sees cross-component
# interleavings no per-package unit test can.
sim-race:
	go test -race -tags sim -timeout 900s ./tests/sim/...

bench:
	go test -bench=. -benchmem ./benchmarks/

# Regenerate the published benchmark dataset (docs/data/): per-operation
# latency percentiles with provenance, for the frontend to consume. Run on a
# known machine, not shared CI - the numbers are meant to be citable.
bench-report:
	go run ./cmd/benchreport -out docs/data

# CPU-bound benchmark subset for regression gating: excludes fsync-bound,
# cluster-setup, and multi-millisecond benchmarks whose wall time is dominated
# by IO or fixture setup and therefore too noisy for CI comparison.
bench-ci:
	go test ./benchmarks/ -run 'XXX' -bench . \
		-skip 'Durable|Cluster|StoreScan100|TombstoneGC|ManagerRebuild' \
		-count=8 -benchtime=0.3s

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
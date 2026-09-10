.PHONY: build run test test-race e2e e2e-integration fault sim sim-race bench bench-ci bench-report lint workflow-lint patch-coverage docker clean

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

# CPU-bound benchmark subset for regression gating. The selection lives in the
# script so this target and the CI gate's interleaved A/B run measure exactly
# the same benchmarks.
bench-ci:
	bash scripts/bench-ab.sh --single

lint:
	golangci-lint run ./...

# Workflow files are configuration GitHub parses on its own: a syntax error in
# one does not fail a run, it makes the workflow unreadable, so no job starts
# and nothing goes red. Lint them like code.
workflow-lint:
	go -C tools install github.com/rhysd/actionlint/cmd/actionlint
	actionlint -shellcheck= -pyflakes=

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Diff coverage against BASE (default origin/main), the same gate CI runs on a
# pull request. Codecov reports this number but cannot block on it - its patch
# status is informational - so the gate lives here instead.
#
# This target owns the git invocation for both CI and local runs, so the
# merge-base semantics live in one place. The three dots matter: they diff
# against the merge base, so commits landing on BASE after this branch forked
# are not attributed to it.
BASE ?= origin/main
patch-coverage:
	go test -coverprofile=coverage.out -covermode=atomic ./...
	git diff --unified=0 $(BASE)...HEAD -- '*.go' | \
		go run ./cmd/patchcov -profile coverage.out -min 80

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
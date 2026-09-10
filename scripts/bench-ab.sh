#!/usr/bin/env bash
# bench-ab.sh --single                                  # run the gate's subset once
# bench-ab.sh <base-sha> <head-sha> <base-out> <head-out>  # interleaved A/B
#
# Owns the benchmark subset the CI regression gate measures, so `make bench-ci`
# and the gate itself can never drift apart.
#
# The subset excludes fsync-bound, cluster-setup, and multi-millisecond
# benchmarks whose wall time is dominated by IO or fixture setup and is
# therefore too noisy to compare on a shared runner.
#
# Why interleave: running every base sample and then every head sample makes
# the comparison hostage to whatever else the runner is doing. Drift over the
# job's lifetime - a noisy neighbour arriving, thermal throttling - lands
# entirely on whichever side ran second, and benchstat reports it as a
# significant one-sided regression. That is not hypothetical: a dependency bump
# touching no ring code failed the gate at RemoveNode +24.91%, head variance
# 13% against base's 6%. Alternating rounds spreads drift across both sides,
# where it reads as variance and benchstat's significance test absorbs it.
#
# Each commit is compiled to a test binary once, up front, and the binaries are
# then run round-robin. No git checkout inside the measurement loop, nothing to
# recompile between rounds, and the base commit needs no build tooling of its
# own - only its source.
set -euo pipefail

# The gate's benchmark selection. Single source of truth: `make bench-ci`
# delegates here rather than repeating these flags.
BENCH_SKIP='Durable|Cluster|StoreScan100|TombstoneGC|ManagerRebuild'
BENCH_TIME="${BENCH_TIME:-0.3s}"
BENCH_COUNT="${BENCH_COUNT:-8}"

if [[ "${1:-}" == "--single" ]]; then
    exec go test ./benchmarks/ -run 'XXX' -bench . \
        -skip "$BENCH_SKIP" -count="$BENCH_COUNT" -benchtime="$BENCH_TIME"
fi

if [[ $# -ne 4 ]]; then
    echo "usage: $0 --single | $0 <base-sha> <head-sha> <base-out> <head-out>" >&2
    exit 2
fi

# Checking out the base commit swaps this file out from under the running
# shell, which reads scripts incrementally. Re-exec from a copy outside the
# work tree so the checkout below cannot corrupt execution mid-run.
if [[ "${BENCH_AB_REEXEC:-}" != "1" ]]; then
    BENCH_AB_SELF="$(mktemp -t bench-ab)"
    cp "$0" "$BENCH_AB_SELF"
    # exec replaces this shell, so an EXIT trap here would never fire; the copy
    # is removed by the re-exec'd child's trap instead.
    export BENCH_AB_SELF
    BENCH_AB_REEXEC=1 exec bash "$BENCH_AB_SELF" "$@"
fi

base_sha="$1"
head_sha="$2"
base_out="$3"
head_out="$4"

workdir="$(mktemp -d -t bench-ab)"
# Always land back on head: the gate compares from there, and a job left on a
# detached base commit would silently measure the wrong tree afterwards.
trap 'git checkout -q "$head_sha"; rm -rf "$workdir" "${BENCH_AB_SELF:-}"' EXIT

compile() {
    local sha="$1" out="$2"
    git checkout -q "$sha"
    go test -c -o "$out" ./benchmarks/
}

compile "$base_sha" "$workdir/base.bin"
compile "$head_sha" "$workdir/head.bin"

run_round() {
    local bin="$1" out="$2"
    "$bin" -test.run='XXX' -test.bench=. \
        -test.skip="$BENCH_SKIP" -test.count=1 -test.benchtime="$BENCH_TIME" >>"$out"
}

: >"$base_out"
: >"$head_out"

for round in $(seq 1 "$BENCH_COUNT"); do
    echo "round $round/$BENCH_COUNT" >&2
    run_round "$workdir/base.bin" "$base_out"
    run_round "$workdir/head.bin" "$head_out"
done

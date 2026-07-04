#!/usr/bin/env bash
# benchguard.sh <base-bench.txt> <head-bench.txt>
#
# Compares two `go test -bench` outputs with benchstat and fails when any
# benchmark shows a statistically significant time regression above
# BENCH_REGRESSION_THRESHOLD percent (default 20).
#
# benchstat only prints a percentage delta when the difference is significant
# at its default alpha (insignificant rows show "~"), so every "+N%" it emits
# is already noise-filtered; the threshold on top of that absorbs the residual
# run-to-run variance of shared CI runners. The geomean row is skipped so an
# accumulation of small, individually-insignificant shifts cannot fail the
# gate on its own.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <base-bench.txt> <head-bench.txt>" >&2
    exit 2
fi

threshold="${BENCH_REGRESSION_THRESHOLD:-20}"
out="$(benchstat "$1" "$2")"
echo "$out"

regressions="$(echo "$out" | awk -v t="$threshold" '
    /geomean/ { next }
    {
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^\+[0-9.]+%$/) {
                v = substr($i, 2, length($i) - 2) + 0
                if (v > t) { print "  " $1 " " $i }
            }
        }
    }')"

if [[ -n "$regressions" ]]; then
    echo ""
    echo "FAIL: significant benchmark regressions above ${threshold}%:"
    echo "$regressions"
    exit 1
fi
echo ""
echo "OK: no significant regressions above ${threshold}%."

#!/usr/bin/env bash
# trace-conformance.sh
#
# Records an execution of the real cluster and makes TLC replay it against the
# model. Fails when the implementation did something specs/Replication.tla does
# not permit.
#
# Two steps: TestTraceConformance (tests/sim) drives a cluster and writes
# specs/TraceData.tla, then TLC replays it. The trace is regenerated every run
# rather than committed, so this checks the implementation as it is now.
#
# Reading TLC's result needs care, because success here looks like failure.
# An observation can be explained by more than one action, so the replay is a
# search; the question is whether *any* path explains the whole trace, which is
# reachability rather than invariance. TraceIncomplete asserts the opposite of
# what we want, so TLC violating it is the witness of success, and TLC finding
# no violation means no path ever explained the final event. This script exists
# so that inversion lives in one documented place instead of in a CI step that
# silently means the opposite of what it appears to.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> recording a trace from the simulation harness"
go test -count=1 -tags sim -run TestTraceConformance ./tests/sim/

if [[ ! -f specs/TraceData.tla ]]; then
    echo "the scenario did not produce specs/TraceData.tla" >&2
    exit 1
fi

echo
echo "==> replaying it against the model"
# -deadlock: branches that pick a different (also legal) explanation for an
# observation can dead-end; that is the search working, not a failure.
out="$(bash scripts/tlc.sh Trace -deadlock 2>&1 || true)"

# Order matters: a real safety violation must be reported as one even though
# the witness invariant may also have been violated in some other branch.
if grep -q "Invariant NoResurrection is violated" <<<"$out"; then
    echo "$out"
    echo
    echo "FAIL: the recorded execution reached a state violating NoResurrection." >&2
    echo "The implementation broke a property the model guarantees." >&2
    exit 1
fi

if grep -q "Invariant TypeOK is violated" <<<"$out"; then
    echo "$out"
    echo
    echo "FAIL: the replay produced a state outside the model's type invariant." >&2
    echo "Usually the trace recorder and the spec have drifted apart." >&2
    exit 1
fi

if grep -q "Invariant TraceIncomplete is violated" <<<"$out"; then
    events="$(grep -c 'kind |->' specs/TraceData.tla || echo '?')"
    echo "OK: every one of the $events recorded events is explained by a legal step."
    exit 0
fi

echo "$out"
echo
echo "FAIL: no execution of the model explains the recorded trace." >&2
echo "The implementation did something specs/Replication.tla does not permit;" >&2
echo "the states above are as far as any replay got." >&2
exit 1

#!/usr/bin/env bash
# tlc.sh <config> [extra tlc args...]
#
# Runs the TLA+ model checker over specs/Replication.tla with the named
# configuration (e.g. `tlc.sh Replication` or `tlc.sh Strict`).
#
# TLC ships as a Java jar rather than something installable from a package
# manager, so this caches it under .tlc/ on first use. The version is pinned:
# a checker that silently changed under us would make "the spec still checks"
# a much weaker statement.
#
# Exit status is TLC's: non-zero when an invariant is violated, which is what
# lets this be a CI gate rather than a report.
set -euo pipefail

TLA_VERSION="${TLA_VERSION:-v1.8.0}"
CACHE_DIR="${TLC_CACHE_DIR:-.tlc}"
JAR="$CACHE_DIR/tla2tools-${TLA_VERSION}.jar"
SPEC_DIR="${SPEC_DIR:-specs}"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <config-name> [tlc args...]" >&2
    echo "  e.g. $0 Replication" >&2
    exit 2
fi

config="$1"
shift

if [[ ! -f "$SPEC_DIR/$config.cfg" ]]; then
    echo "no such config: $SPEC_DIR/$config.cfg" >&2
    exit 2
fi

if ! command -v java >/dev/null 2>&1; then
    echo "TLC needs a JVM; install a JDK (17 or newer) and retry." >&2
    exit 2
fi

if [[ ! -f "$JAR" ]]; then
    mkdir -p "$CACHE_DIR"
    url="https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"
    echo "fetching tla2tools ${TLA_VERSION}..."
    curl -sSL --fail -o "$JAR.tmp" "$url"
    mv "$JAR.tmp" "$JAR"
fi

echo "checking $config.cfg"
# -workers auto uses every core; the parallel collector keeps the heap from
# dominating on the larger configurations.
( cd "$SPEC_DIR" && exec java -XX:+UseParallelGC \
    -cp "../$JAR" tlc2.TLC \
    -config "$config.cfg" \
    -workers auto \
    "$@" \
    Replication.tla )

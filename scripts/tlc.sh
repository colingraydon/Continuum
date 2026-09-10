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
# Pinned alongside the version. This script downloads a jar and then executes
# it, so the version tag alone is not enough: a re-cut release, a compromised
# asset, or a redirect that downgraded the transport would all go unnoticed
# without checking what actually arrived. Same reasoning as --require-hashes in
# requirements-docs.txt. Update both together when bumping TLA_VERSION.
TLA_SHA256="${TLA_SHA256:-957b23b2bb31d08f19346e105e23585f93fea9a139a712b0ac347eedaf26afea}"
CACHE_DIR="${TLC_CACHE_DIR:-.tlc}"
JAR="$CACHE_DIR/tla2tools-${TLA_VERSION}.jar"
SPEC_DIR="${SPEC_DIR:-specs}"

# sha256 of a file, portable across the macOS and Linux toolchains.
sha256_of() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | cut -d' ' -f1
    else
        shasum -a 256 "$file" | cut -d' ' -f1
    fi
}

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
    # --proto '=https' applies to redirects too, so -L cannot be walked down to
    # plaintext by a redirect chain; the checksum below is the real guarantee.
    curl -sSL --fail --proto '=https' --tlsv1.2 -o "$JAR.tmp" "$url"

    got="$(sha256_of "$JAR.tmp")"
    if [[ "$got" != "$TLA_SHA256" ]]; then
        rm -f "$JAR.tmp"
        echo "tla2tools ${TLA_VERSION} checksum mismatch - refusing to run it." >&2
        echo "  expected: $TLA_SHA256" >&2
        echo "  got:      $got" >&2
        exit 1
    fi
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

#!/usr/bin/env bash
# sonar-branch-gate.sh <commit-sha>
#
# Fails when SonarQube Cloud's *branch* quality gate is red for this commit.
#
# Why this exists: the required "SonarCloud Code Analysis" check only ever
# validates a pull request's diff. After a merge, Sonar runs a second, different
# analysis against the branch, scoped to the project's new-code window - and
# that one can fail where the PR passed. It cannot gate anything, because it
# only exists once the merge has already happened. So `main` can sit red with
# every check on the merge commit green, which is exactly what happened after
# #90: three hours red, unnoticed, because nothing was watching.
#
# This turns that silence into a red CI run.
#
# It binds the verdict to a specific commit rather than asking "is the branch
# red right now": the analysis lands seconds after the push, so a naive query
# would read the *previous* commit's verdict and report the wrong answer. Each
# analysis carries the git revision it ran on, so this waits for one matching
# our SHA and then asks for that analysis by ID.
#
# A missing analysis is not a failure. Automatic Analysis may legitimately skip
# a commit that touches no analyzable source (a docs-only merge), and failing
# CI for that would make every docs change red. Timing out reports loudly and
# exits 0; only an actual ERROR verdict fails.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <commit-sha>" >&2
    exit 2
fi

sha="$1"
project="${SONAR_PROJECT_KEY:-colingraydon_Continuum}"
host="${SONAR_HOST:-https://sonarcloud.io}"
branch="${SONAR_BRANCH:-main}"
timeout_s="${SONAR_GATE_TIMEOUT_S:-420}"
poll_s="${SONAR_GATE_POLL_S:-15}"

# The API is public for public projects; a token is only needed for private
# ones. Passing an empty -u would break the request, so only set it when present.
curl_auth=()
if [[ -n "${SONAR_TOKEN:-}" ]]; then
    curl_auth=(-u "${SONAR_TOKEN}:")
fi

# The ${arr[@]+"${arr[@]}"} form expands to nothing when the array is empty
# instead of tripping `set -u` on bash 3.2, which is what a contributor on macOS
# would hit even though the CI runner's bash 5 is fine with the plain form.
api() { curl -sS --max-time 30 ${curl_auth[@]+"${curl_auth[@]}"} "$@"; }

echo "Waiting for a SonarQube Cloud analysis of ${branch} at ${sha:0:8} (up to ${timeout_s}s)..."

analysis_id=""
deadline=$(( SECONDS + timeout_s ))
while (( SECONDS < deadline )); do
    # A transient API blip should retry, not kill the job.
    body="$(api "${host}/api/project_analyses/search?project=${project}&branch=${branch}&ps=5" || true)"
    if [[ -n "$body" ]]; then
        analysis_id="$(jq -r --arg sha "$sha" \
            'first(.analyses[]? | select(.revision == $sha) | .key) // empty' <<<"$body" 2>/dev/null || true)"
        if [[ -n "$analysis_id" ]]; then
            break
        fi
        latest="$(jq -r 'first(.analyses[]?) | "\(.date) \(.revision // "?")"' <<<"$body" 2>/dev/null || true)"
        echo "  no analysis for this commit yet (latest: ${latest:-unknown})"
    fi
    sleep "$poll_s"
done

if [[ -z "$analysis_id" ]]; then
    echo ""
    echo "NOTE: no SonarQube Cloud analysis for ${sha:0:8} within ${timeout_s}s."
    echo "Automatic Analysis skips commits with no analyzable source change, so this"
    echo "is expected for a docs-only or workflow-only merge. Not failing the build."
    exit 0
fi

status_body="$(api "${host}/api/qualitygates/project_status?analysisId=${analysis_id}")"
status="$(jq -r '.projectStatus.status' <<<"$status_body")"

echo ""
echo "Branch quality gate: ${status}"

if [[ "$status" == "ERROR" ]]; then
    echo ""
    echo "Failed conditions:"
    jq -r '.projectStatus.conditions[]
           | select(.status == "ERROR")
           | "  \(.metricKey): \(.actualValue) (must not be \(.comparator) \(.errorThreshold))"' <<<"$status_body"
    echo ""
    echo "Dashboard: ${host}/dashboard?id=${project}&branch=${branch}"
    exit 1
fi

echo "OK: branch gate is green for ${sha:0:8}."

# Contributing to Continuum

Continuum is a distributed key-value store built to be **read and learned from**.
Correctness and clarity matter more than velocity here: a change that works but
leaves the docs or the next reader behind is not done.

This guide sets a few lightweight conventions. There is deliberately no heavy
process — but the two things below are firm:

1. **Docs are part of the change, not a follow-up** (see [Documentation](#documentation-is-required)).
2. **Every PR follows the [PR format](#pull-request-format).**

## Development setup

Requires Go (see the version in [`go.mod`](go.mod)).

```bash
git clone https://github.com/colingraydon/Continuum && cd Continuum
make run        # build and run a single node
make test       # unit + integration tests
```

Common targets (full list in [`docs/operations.md`](docs/operations.md)):

| Target | What it runs |
| ------ | ------------ |
| `make test` | Unit + integration tests |
| `make test-race` | Same, under the race detector |
| `make lint` | `golangci-lint` |
| `make e2e` | In-process cluster tests |
| `make fault` | Process-based fault-injection suite (kills, hangs, partitions, packet loss) |
| `make sim` / `make sim-race` | Seeded in-process cluster simulation |
| `make coverage` | HTML coverage report |
| `make bench-ci` | The benchmark subset the CI regression gate uses |

## Before you open a PR

Run these locally and make sure they pass — CI runs the same gates, so catching
them here saves a round trip:

- [ ] `make test` (and `make test-race` if you touched anything concurrent)
- [ ] `make lint` — **zero issues**
- [ ] `gofmt` clean on the files you changed
- [ ] `make fault` and/or `make sim` when the change touches replication,
      consistency, gossip, or membership — the behavior these harnesses exist to
      protect
- [ ] Tests added for new logic; coverage stays high (the repo tracks coverage
      via Codecov)
- [ ] **Docs updated** (see below)

CI additionally runs `go vet`, the full test suite with coverage, e2e, fault
injection, CodeQL, a benchmark regression guard (fails on >20% significant
regressions), and a `mkdocs --strict` docs build. SonarCloud flags functions
whose **cognitive complexity exceeds 15** — keep new functions under it.

## Documentation is required

**A change that alters behavior or architecture must update the docs in the same
PR.** Code and docs live and move together; a PR that changes what the system
does without touching the docs will be sent back.

Two surfaces, both mandatory when relevant:

- **`README.md`** — update it when the change affects what the project *does* or
  *claims*: capabilities, the headline behavior, benchmark numbers, or the
  **"What's Next"** roadmap (move a shipped item out; add follow-ups you
  deferred).
- **`docs/*.md`** (the architectural docs) — update the doc for the subsystem you
  touched (`ring.md`, `replication.md`, `gossip.md`, `persistence.md`, …). A new
  subsystem gets a **new doc**, added to the `nav:` in [`mkdocs.yml`](mkdocs.yml).
  Non-trivial features get a **design doc** under `docs/` recording the decisions
  and any deferred work up front (see [`docs/paxos-cas-design.md`](docs/paxos-cas-design.md)
  for the shape).

If a PR ships something partial (one slice of a larger feature), the docs must
say what is **shipped** versus **planned** so they never overclaim.

`mkdocs build --strict` must pass — it catches broken links and orphaned nav
entries.

## Pull request format

Write the PR description with these five sections. Keep each tight; a reviewer
should understand the shape of the change before reading a line of the diff.

```markdown
## Summary
One or two sentences: what this PR does and why it exists.

## What changed
The concrete changes, as bullets — the code, the config, the docs. Enough that a
reviewer knows where to look.

## Review help
Where to start reading, the load-bearing or subtle parts, and anything you're
unsure about or want a second opinion on. Point reviewers at what matters.

## Verification
What you ran and the result — tests, lint, coverage, e2e/fault/sim, any manual
check — and how a reviewer can reproduce it.

## Next steps
Omit for standalone PRs. For a staged series, list the ordered arc and mark where
this PR sits (e.g. "PR 2 of 5"), so reviewers know later PRs build on it and must
land in order.
```

### Staged / multi-PR work

Large features land as an **ordered series of small PRs** rather than one wall of
diff. Each PR in the series should:

- be self-contained and **green on its own** (build, tests, lint all pass);
- keep the docs honest about what it ships versus what is still planned;
- reference the arc in its **Next steps** section and land in order.

Record the whole arc and any deferred optimizations in a design doc under `docs/`
in the first PR, so the plan and its boundaries are visible from the start.

## Commits

Subjects follow conventional-commit prefixes — `feat:`, `fix:`, `docs:`,
`refactor:`, `test:`, `chore:` — written in the imperative. Keep the subject a
readable summary; put the detail in the body.

## Code style

There is no bespoke style guide: **`gofmt` and `golangci-lint` are the
authority.** Beyond that, match the surrounding code — its naming, comment
density, and error handling. Prefer reusing an existing helper over adding a
parallel one. Keep functions under cognitive complexity 15, and write comments
that explain *why*, not *what*.

# Security Posture

> What Continuum does and does not defend against, stated plainly. This is a
> distributed key-value store built to be read and learned from; the posture
> below is a deliberate scope decision, not a claim of hardening.

## Trust model

**Continuum assumes a trusted network.** Every node is assumed to be operated by
the same party, on a network where an attacker can neither read nor inject
traffic. Concretely:

- **Node-to-node traffic is plaintext HTTP.** Replica writes, quorum reads, read
  repair, anti-entropy sync, paxos phases, hint delivery, and migration all ride
  unencrypted HTTP.
- **There is no authentication between nodes.** Any host that can reach a node's
  HTTP port can register itself into the ring (`POST /nodes`), read any key, or
  write to any key as a replica sub-write.
- **The client API is unauthenticated plaintext HTTP.** There are no users,
  tokens, roles, or per-key access control.
- **Gossip is unauthenticated UDP.** A peer that can reach the gossip port can
  assert membership state, including marking other nodes dead.

Deploy it accordingly: inside a private network or VPC, never exposed to the
public internet, with network-level controls (security groups, service mesh,
mTLS sidecars) providing whatever isolation the deployment needs.

## What this means for the static analysis findings

SonarQube Cloud raises **`go:S5332`** ("Using HTTP protocol is insecure. Use
HTTPS instead.") against the `http://` scheme literals in the node-to-node
paths. Those findings are accurate — the traffic really is plaintext — and they
describe the documented posture above rather than a defect.

Rather than mute them out of sight, each package routes every `http://` through
a single `schemeHTTP` constant carrying the rationale and a `//NOSONAR`:

| Package | Constant |
| ------- | -------- |
| `api` | `api/handlers.go` |
| `internal/antientropy` | `internal/antientropy/manager.go` |

Two consequences worth understanding:

- The suppression is **declarative and reviewable** — it lives next to the
  decision, in the repository, rather than in a SonarCloud project setting no
  reviewer sees. Automatic Analysis cannot express rule-level exclusions in
  `.sonarcloud.properties` (it supports only path scoping), and excluding those
  files wholesale would drop the three most important files in the codebase from
  analysis entirely.
- `//NOSONAR` is blunt: it suppresses **any** issue Sonar raises on that line,
  not just `go:S5332`. That is tolerable on a line that is a single string
  constant and nothing else, which is part of why the literals were centralized.

When transport security lands, both constants and this section go away together.

## Not in scope today

Each of these is a real gap, listed so the boundary is explicit rather than
implied:

- **Transport security between nodes** (TLS, or mTLS with per-node certificates)
  — the item that would retire the `go:S5332` suppressions.
- **Client authentication and authorization** — no identities, no per-key ACLs.
- **Cluster join authorization** — `POST /nodes` accepts any caller, so ring
  membership is only as safe as the network.
- **Gossip authentication** — no shared secret or signature over membership
  claims, so a reachable attacker can drive the failure detector.
- **Encryption at rest** — SSTables, the WAL, and the hint log are written in
  the clear; use disk-level encryption where that matters.
- **Rate limiting and quotas** — there is no admission control on the request
  path (see the backpressure item in the README roadmap).

## See Also

- [Operations](operations.md) — deployment and configuration
- [Architecture](architecture.md) — the components these paths run between

# Multi-DC Replication

> Promoting **data center** to a first-class failure domain above the zone, so a
> key can keep full replica sets in more than one DC and clients can trade
> cross-DC latency for availability per request.

## Status

This is a staged feature. **PRs 1–2 are shipped:** the `DC` label propagates
through gossip and is surfaced on `/stats` (PR 1), and per-DC replica counts
now drive placement (PR 2). **Quorum is still cluster-wide** — `LOCAL_QUORUM`,
`LOCAL_ONE`, and async cross-DC delivery land in later PRs; see
[PR staging](#pr-staging).

## Why

Continuum already spreads a key's replicas across failure-domain *zones* (rack,
AZ) so losing one zone costs at most one replica (see [Ring](ring.md)). A data
center is the next failure domain up: a whole-DC outage or a severed WAN link
between DCs should not take a key offline, and a client in one DC should not pay
a cross-continent round trip on every quorum.

Multi-DC replication makes the DC a hierarchical dimension *above* the zone:

- **Per-DC replica counts** — instead of one cluster-wide `RF`, a key keeps a
  configured number of replicas *in each DC* (e.g. 3 in `us-east`, 3 in
  `eu-west`), so either DC can serve the key alone.
- **`LOCAL_QUORUM` / `LOCAL_ONE`** — a quorum counted only among the
  coordinator's *local* DC replicas, so writes and reads never block on a slow
  or unreachable remote DC.
- **Async cross-DC replication** — the coordinator acks the client once the
  local DC is satisfied and delivers to remote DCs in the background, reusing
  the existing fan-out and hinted-handoff machinery.

Concurrent cross-DC writes need no new conflict model: vector clocks already
turn them into siblings, and anti-entropy already heals divergence.

## Model

`DC` and `Zone` are **separate fields** on `ring.Node` and `gossip.Member`, not
a single hierarchical string. Zone uniqueness is scoped *within* a DC — two
racks named `rack1` in different DCs do not collide.

```
region  ─┐
 DC      ─┤  ← this feature adds the DC level
  zone   ─┤  ← existing failure domain (rack / AZ)
   node  ─┘
```

The label flows exactly like `SELF_ZONE` does: `SELF_DC` → `MemberList.self.DC`
→ gossip → each peer's ring via the membership callback. `gossip.Member` is the
gossip wire struct (JSON-encoded by field name, no tags), so the field rides the
wire with no serialization changes, and older nodes that omit it degrade
gracefully to an empty DC.

## Locked design decisions

| Decision | Choice | Rationale |
| -------- | ------ | --------- |
| DC representation | Separate `DC` field, not a hierarchical zone string | Keeps the two failure domains independent and each dimension simple |
| Zone scope | Per-DC (zone uniqueness scoped within a DC) | Matches Cassandra's NetworkTopologyStrategy; DC and zone stay cleanly hierarchical |
| Per-DC replica counts | Static `REPLICATION_FACTOR_BY_DC`, identical on every node | Keeps placement a deterministic pure function of ring + membership, like `RF` today |
| Cross-DC delivery | Direct fan-out to remote replicas, reusing fan-out + hinted handoff | Simplest correct path; forwarding to a remote-DC coordinator is a later bandwidth optimization |

## PR staging

1. **PR 1 — `DC` label + plumbing (shipped).** `DC` on `ring.Node` /
   `gossip.Member`, `SetSelfDC`, `SELF_DC` env, gossip propagation (including the
   metadata-change re-fire), and `/stats` surfacing. No behavior change.
2. **PR 2 — DC-aware placement (shipped).** `SetDCReplication` /
   `REPLICATION_FACTOR_BY_DC`, the per-DC replica walk with per-DC zone
   spreading, and DC-scoped sloppy-quorum substitution.
3. **PR 3 — `LOCAL_QUORUM` / `LOCAL_ONE`.** Partition replica acks by DC in the
   read and write paths; add the consistency levels.
4. **PR 4 — Async cross-DC replication.** Ack the client on local-DC quorum,
   fan out to remote DCs without blocking, hint remote failures, tune AE cadence
   for the WAN.
5. **PR 5 (optional) — Verification.** Add a DC dimension and a full
   cross-DC-partition scenario to the fault and simulation harnesses, asserting
   `LOCAL_QUORUM` stays available when the remote DC is unreachable.

## What is shipped vs. what is planned

**Ships now (PRs 1–2):** the `DC` field, `SELF_DC`, gossip propagation,
`/stats.dc`; and `REPLICATION_FACTOR_BY_DC` driving placement — per-DC replica
targets, zone spreading scoped inside each DC, and sloppy-quorum substitutes
confined to the failed owner's DC.

**Not yet:** quorum is still **cluster-wide**. With `us-east:3,eu-west:3` the
effective RF is 6 and the default quorum is 4, so a write must still be
acknowledged across both DCs — the coordinator cannot yet be satisfied by its
local DC alone. `LOCAL_QUORUM`, `LOCAL_ONE`, and async cross-DC delivery land in
PRs 3–4. Until then, per-DC placement buys **durability** across DCs but not the
**latency or availability** win; a fully unreachable DC will fail quorum even
though the local replica set is intact.

### Configuring it

```bash
SELF_DC=us-east SELF_ZONE=rack1 \
REPLICATION_FACTOR_BY_DC=us-east:3,eu-west:3 \
./continuum
```

The table is static config and must be identical on every node. A DC absent from
it holds no replicas, so it must name every DC that carries data; startup fails
if `SELF_DC` is empty or unlisted. See
[operations](operations.md#environment-variables) for the full variable
reference.

## Deferred / future work

Recorded up front so the arc's boundaries are explicit; each is intentionally
out of scope for the initial implementation:

1. **Gossiped runtime topology map.** Per-DC replica counts start as static
   config identical on every node. A gossiped, versioned topology map would let
   the counts change at runtime without redeploying, at the cost of a
   convergence/consistency problem layered onto placement determinism.
2. **Forward-to-remote-DC coordinator.** v1 fans out directly to every remote
   replica (one WAN round trip each). A Cassandra-style design forwards a single
   copy to one delegate per remote DC, which fans out locally — saving WAN
   bandwidth on high replica counts, at the cost of an extra endpoint and a
   second failure point.
3. **`EACH_QUORUM`.** A write consistency level requiring a quorum in *every*
   DC (vs. `LOCAL_QUORUM`'s single-DC majority). Useful for strong multi-DC
   durability guarantees; deferred because it reintroduces cross-DC blocking.
4. **Cross-DC anti-entropy cadence tuning.** Anti-entropy is WAN-cost-unaware
   today; cross-DC repair should run on its own (slower) cadence and possibly
   prefer a local-DC peer before reaching across the WAN.
5. **Whole-DC-outage hint volume.** A prolonged DC or WAN outage can overrun the
   in-memory hint cap (10k/node). Anti-entropy is the backstop, but a robust
   story may need hint overflow-to-disk or a bulk cross-DC re-sync path.

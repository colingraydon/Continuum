# Multi-DC Replication

> Promoting **data center** to a first-class failure domain above the zone, so a
> key can keep full replica sets in more than one DC and clients can trade
> cross-DC latency for availability per request.

## Status

**Shipped.** The `DC` label propagates through gossip and is surfaced on
`/stats` (PR 1), per-DC replica counts drive placement (PR 2), `local_quorum` /
`local_one` scope acknowledgements to the coordinator's own DC (PR 3), and both
verification harnesses now carry a DC dimension with cross-DC partition
scenarios (PR 5).

PR 4 — cross-DC delivery hardening — has now landed too: replica hops across
the WAN get their own timeout budget, anti-entropy repairs remote DCs on its own
slower cadence, and hints dropped undelivered escalate to a targeted repair
instead of waiting on the background cycle. See
[Cross-DC delivery](#cross-dc-delivery) for the mechanisms and
[Deferred / future work](#deferred-future-work) for what remains.

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
3. **PR 3 — `LOCAL_QUORUM` / `LOCAL_ONE` (shipped).** Replica acks partitioned
   by DC in the read and write paths; both consistency levels added.
4. **PR 4 — Cross-DC delivery hardening (shipped, after PR 5).** Acking on
   local-DC quorum while the remote fan-out continues in the background *already
   fell out of PR 3*. This PR is the durability story around that: a WAN timeout
   budget so healthy remote replicas stop being timed out and hinted, a
   WAN-paced anti-entropy cadence, and escalation of dropped hints to a targeted
   repair. Landed out of order — PR 5's scenarios came first and are what the
   overflow scenario here was built on top of.
5. **PR 5 — Verification (shipped).** A DC dimension in both the simulation and
   fault harnesses, plus cross-DC partition scenarios asserting `local_quorum`
   stays available while the remote DC is unreachable and that writes accepted
   during the cut reach it afterwards.

## What is shipped vs. what is planned

**Ships now (PRs 1–3):** the `DC` field, `SELF_DC`, gossip propagation,
`/stats.dc`; `REPLICATION_FACTOR_BY_DC` driving placement — per-DC replica
targets, zone spreading scoped inside each DC, and sloppy-quorum substitutes
confined to the failed owner's DC; and the `local_quorum` / `local_one`
consistency levels, which count acks only within the coordinator's DC.

With `us-east:3,eu-west:3` the cluster RF is 6, so `quorum` needs 4 acks
spanning both DCs while `local_quorum` needs 2 from `us-east` alone. A severed
WAN link or a whole-DC outage now fails the former and survives the latter —
that is the **latency and availability** win, on top of the cross-DC
**durability** PR 2 delivered.

**Verified (PR 5):** both harnesses run a two-DC topology. The simulation
harness cuts the WAN between data centers with each side still internally
quorate; the fault harness blackholes three real processes to take a whole DC
dark. Both assert that `local_quorum` keeps serving from the surviving side
while a cluster-wide quorum fails, and that every write accepted during the
outage reaches the remote DC once it returns. Each scenario also asserts its
precondition — that the remote DC held none of those writes *during* the cut —
so a leaky partition cannot make the post-heal assertion pass for the wrong
reason.

**Shipped (PR 4):** the cross-DC delivery path is hardened — a WAN timeout
budget, a WAN-paced anti-entropy cadence, and escalation of dropped hints. See
[Cross-DC delivery](#cross-dc-delivery).

**Not yet:** `EACH_QUORUM` does not exist, and the per-DC replica table is still
static config rather than a gossiped topology map. See
[Deferred / future work](#deferred-future-work).

## Cross-DC delivery

A `local_quorum` write returns as soon as the local DC acks and the remote
fan-out continues in the background, so everything that carries a write to the
far side runs after the client is gone. Three things make that path survive a
WAN rather than merely work on a LAN.

### One timeout budget per failure domain

A replica hop to another DC gets `CROSS_DC_REPLICA_TIMEOUT_MS` (default 2s)
instead of `REPLICA_TIMEOUT_MS` (default 500ms). A single budget for both had a
quiet cost: a cross-continent round trip routinely exceeds a budget sized for a
rack neighbour, so healthy remote replicas timed out as a matter of course and
their writes were buffered as hints and retried later — paying the WAN twice to
deliver what waiting a little longer would have delivered once. The budget
applies only between two DC-labeled nodes; an unlabeled cluster is unchanged.

### Anti-entropy pays the WAN less often

Anti-entropy compares one Merkle root per vnode per round, and the overwhelming
majority of those comparisons report "identical". Running that unpaced across a
WAN spends bandwidth to learn nothing, so a replica in another DC is compared
only every `CROSS_DC_SYNC_EVERY` rounds (default 4) while local replicas keep
the full cadence. The round-robin guarantee scales accordingly: a full cross-DC
pass takes `vnodes × SYNC_INTERVAL_MS × CROSS_DC_SYNC_EVERY`, still bounded,
just slower. The first round after startup always crosses the WAN — a node that
just came up is when remote divergence is most likely.

### Dropped hints escalate instead of waiting

Hints are capped per target node and expire on a TTL. Both limits exist to bound
memory, and both mean the same thing when they bite: writes this coordinator
accepted will never reach that replica by the hint path. A prolonged DC outage
hits them by design — hints accumulate at write rate against a fixed cap.

Anti-entropy has always been the documented backstop, but on its own it only
recovers those keys when the round-robin cursor happens upon their vnodes, which
for a remote DC is now the slow cadence times the vnode count. So a drop is now
an event: the hint store reports it, and the coordinator schedules a targeted
anti-entropy pass against that node, deferred until gossip says it is reachable
(escalating into a still-dark DC would burn a vnode sweep on connection
timeouts).

That pass walks the coordinator's **entire replica set**, not the primary subset
the background round drives — a coordinator is primary for only a fraction of
the keys it accepts, so a pass scoped to its primary ranges would leave most of
the dropped writes unrepaired. Pushing from a non-primary is safe for the same
reason the background sync is: reconciliation is by vector clock, so a redundant
push is idempotent.

`HintStore.Lost()` exposes the per-node drop counts behind this, so overflow is
visible rather than silent.

### Known limitation: the coordinator's self-ack

The coordinator counts its own local write toward the quorum even when it is
not one of the key's replicas — long-standing behavior for the cluster-wide
levels, and unchanged here. Under `local_one` this means a coordinator in a DC
with no replica for the key can satisfy the level from a copy that anti-entropy
will later reclaim as stale. Use `local_quorum` where that matters; tightening
the self-ack is deliberately out of scope for this arc, since it would change
existing single-DC write semantics.

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

Clients then opt into DC-scoped acknowledgement per request:

```
PUT /keys/cart?consistency=local_quorum
GET /keys/cart?consistency=local_one
```

Both levels require the configuration above; on a node without it they return
400 rather than quietly degrading to a cluster-wide quorum. The full semantics
are in the [API reference](api.md).

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
4. ~~**Cross-DC anti-entropy cadence tuning.**~~ **Done in PR 4**
   (`CROSS_DC_SYNC_EVERY`). Still open within it: preferring a local-DC peer
   before reaching across the WAN, so a key already repairable from a rack
   neighbour never costs a WAN round trip at all.
5. ~~**Whole-DC-outage hint volume.**~~ **Handled in PR 4** by escalation rather
   than by raising the ceiling: overrunning the cap is now an event that
   schedules a targeted anti-entropy pass, and `HintStore.Lost()` makes the
   drops visible. The cap itself is unchanged, so the writes between the drop
   and the repair still rely on that pass — overflow-to-disk would remove that
   window entirely and remains open.

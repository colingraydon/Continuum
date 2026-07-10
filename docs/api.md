# HTTP API (`api`)

> Transport layer. Exposes all operations over HTTP, instruments via Prometheus, and wires internal packages together.

## Overview

The API package is the integration point. Handlers are thin - they decode the request, delegate to the appropriate internal package, and serialize the response. All business logic lives in the internal packages.

The API also owns two non-trivial behaviors that do not fit cleanly in a single internal package - read repair (which requires access to both the ring and the store) and data migration (which requires access to gossip, the ring, and the store simultaneously).

## Internal Headers and Endpoints

`X-Proxied-From` - set on all replica sub-requests. When a handler receives a request with this header, it treats itself as a replica, not a coordinator. It stores the value directly without fan-out and without buffering hints. Used by coordinator fan-out, hinted handoff delivery, read repair, and anti-entropy sync pushes.

`POST /paxos/prepare`, `POST /paxos/propose`, `POST /paxos/commit` - the replica-side phases of the per-key paxos round behind `?cas=true` writes and `?consistency=serial` reads. A prepare response carries the replica's committed state for the key plus any accepted-but-uncommitted mutation; propose records an accept; commit applies the decided mutation to the store before clearing the round.

## Endpoints

### Keys

**Write a value**
```
PUT /keys/:key
Content-Type: application/json

{"value": "alice"}
```
Returns 204 with an `X-Session-Clock` response header carrying the write's resulting vector clock as a JSON object. Fans out to the replica set. An optional `clocks` field carries an existing vector clock forward. If omitted, the receiving node's clock is used as the base.

**Read a value**
```
GET /keys/:key
```
Returns the merged result across R replicas. If no siblings exist:
```json
{
  "id": "node2",
  "address": "node2:8080",
  "status": "alive",
  "value": "alice"
}
```
If concurrent writes produced siblings:
```json
{
  "id": "node2",
  "address": "node2:8080",
  "status": "alive",
  "siblings": [
    { "value": "alice", "clocks": { "node1": 2 } },
    { "value": "bob",   "clocks": { "node2": 1 } }
  ]
}
```

**Delete a value**
```
DELETE /keys/:key
Content-Type: application/json

{"clocks": {"node1": 2}}
```
Returns 204 with an `X-Session-Clock` header carrying the tombstone's clock. Writes a tombstone sibling at an incremented clock. The tombstone participates in conflict resolution identically to a value write.

**Per-request consistency**

All three key endpoints accept an optional `?consistency=` query parameter that overrides the process-configured quorum (`WRITE_QUORUM` for PUT/DELETE, `READ_QUORUM` for GET) for that request:

| Level | Quorum | Meaning |
| ----- | ------ | ------- |
| `one` | 1 | Fastest; the coordinator's own copy suffices |
| `quorum` | RF/2 + 1 | Majority of the replication factor |
| `all` | RF | Every current replica must respond |
| `serial` | RF/2 + 1 | GET only: linearizable read via a paxos prepare round |

```
PUT /keys/session-token?consistency=all
GET /keys/profile?consistency=one
GET /keys/lock-owner?consistency=serial
```

An unrecognized level returns 400 without any side effect (`serial` on a write is rejected the same way - writes wanting serial semantics use `?cas=true`). Absent, the process default applies. Like the configured W/R, `one`/`quorum`/`all` are clamped to the currently available replica set, so `all` means "all current replicas", not a hard durability floor - see [Replication](replication.md). A `serial` read instead requires a true majority of the key's replica set and fails with a retryable 503 without one; it observes every decided CAS round through majority intersection and finishes any in-flight round before answering. See [Client Consistency](client-consistency.md).

**Conditional writes (CAS)**
```
PUT /keys/:key?cas=true
Content-Type: application/json

{"value": "bob", "clocks": {"node1": 2}}
```
`?cas=true` on PUT or DELETE makes the `clocks` field a precondition: the write is applied only if it causally dominates every committed sibling of the key, and otherwise rejected with 412 instead of creating a sibling. An empty or absent `clocks` field means "expect no current value", so a CAS PUT doubles as insert-if-absent. Every CAS runs one single-decree paxos round among the key's replica set (prepare, propose, commit), so concurrent CAS writes to a key serialize cluster-wide no matter which coordinators they hit: for each generation of a value exactly one racing writer gets 204. Losers get 412 when the coordinator can prove the precondition failed with no side effects, or a retryable 503 when it cannot (quorum unavailable, round contended, or an in-flight round was finished first). Any `cas` value other than `true` or `false` returns 400 without side effects. See [Client Consistency](client-consistency.md) for the protocol and its guarantees.

**Session reads**
```
GET /keys/:key
X-Session-Clock: {"node1": 2}
```
An `X-Session-Clock` request header (the JSON clock returned by a previous write or read of the same key) asks the coordinator to guarantee the result dominates that clock: read-your-writes and monotonic reads. If the initial quorum read does not cover the session clock, the coordinator escalates to every replica; if the cluster still cannot produce a covering result the read fails with 503 rather than silently returning stale data. Every coordinator GET response carries the observed merged clock back in `X-Session-Clock` (including the 404 for a tombstone). A malformed header returns 400.

**Scan keys by prefix**
```
GET /keys?prefix=user:&limit=100&after=user:41
```
Ordered prefix enumeration across the whole cluster: the coordinator fans a local scan to every alive node and merges per-key sibling sets with the same vector clock dominance rules as point reads. `prefix` is required; `limit` defaults to 100 (max 1000); `after` is the exclusive resume cursor from a previous page's `next`.

```json
{
  "items": [
    { "key": "user:42", "value": "alice" },
    { "key": "user:43", "siblings": [ { "value": "bob", "clocks": {"n1": 2} },
                                      { "value": "carol", "clocks": {"n2": 1} } ] }
  ],
  "next": "user:43"
}
```

An empty `next` means the scan is complete. If any alive node fails to respond the scan returns 503 rather than a partial result. With `X-Proxied-From` set, the endpoint returns the node-local scan (raw sibling sets, tombstones included) - the internal format the coordinator consumes. See [Range Scans](range-scans.md) for the merge and pagination semantics.

### Nodes

**Add a node**
```
POST /nodes
Content-Type: application/json

{"id": "node1", "address": "node1:8080"}
```
Registers the node in the gossip member list and the ring. Triggers a gossip exchange.

**Remove a node**
```
DELETE /nodes/:id
```
Marks the node as dead in the member list and removes it from the ring.

**List all nodes**
```
GET /nodes
```
Returns all known members with their current gossip status.

**Get replication nodes for a key**
```
POST /replicate
Content-Type: application/json

{"key": "user:123", "factor": 3}
```
Returns the N nodes that own replicas of this key. Useful for topology inspection and debugging.

### Health and Stats

**Health check**
```
GET /health
```
```json
{
  "status": "ok",
  "total_nodes": 3,
  "healthy_nodes": 3,
  "suspect_nodes": 0,
  "dead_nodes": 0,
  "uptime": "4h32m10s"
}
```

**Ring and membership stats**
```
GET /stats
```
```json
{
  "total_nodes": 3,
  "total_vnodes": 450,
  "healthy_nodes": 2,
  "suspect_nodes": 1,
  "dead_nodes": 0,
  "distribution": [
    {
      "node_id": "node1",
      "address": "node1:8080",
      "vnode_count": 150,
      "key_count": 342,
      "percentage": 34.2
    }
  ],
  "most_loaded": "node1",
  "least_loaded": "node3",
  "variance": 4.22
}
```

**Prometheus metrics**
```
GET /metrics
```
Standard Prometheus text format. See [Operations](operations.md) for the full metric list.

### Gossip

**Exchange gossip state**
```
POST /gossip
Content-Type: application/json

{"members": [...]}
```
Used internally by the gossip protocol. Merges the provided member list with the local view and returns this node's current member list. Not intended for direct client use.

### Anti-Entropy Sync

These endpoints are used by the anti-entropy manager during background sync. They are HTTP endpoints on the standard port, not a separate service.

**Get sync state for a vnode range**
```
GET /sync?vnode=<endHash>
```
Returns the Merkle tree state for the vnode range ending at `endHash`, served from the incrementally maintained tree for that range (falling back to a store scan when no tree is held yet):
```json
{
  "root": 3829104721,
  "buckets": [1234567890, 0, 987654321, ...]
}
```

**Fetch keys in a sync bucket**
```
GET /sync/bucket-keys?vnode=<endHash>&bucket=<0-15>
```
Returns the key names held in a specific Merkle bucket within a vnode range. Used by the primary during bidirectional sync to discover keys the replica holds that the primary does not.

**Fetch full entries for specific keys**
```
POST /sync/keys
Content-Type: application/json

{"keys": ["user:123", "user:456"]}
```
Returns the full sibling sets for the requested keys, including vector clocks and tombstone state.

**Push entries to a replica**
```
POST /sync/push
Content-Type: application/json

{"entries": {"user:123": [{"value": "alice", "clocks": {"node1": 1}}]}}
```
Applies a batch of entries to the local store. The replica merges each entry through the standard vector clock conflict resolution path.

### Hinted Handoff

Hint delivery has no HTTP endpoint. It is triggered internally two ways: by the gossip `onChange` callback when a node transitions to alive, and by a periodic sweep (`HINT_DELIVERY_INTERVAL_MS`) that delivers to any currently-alive target. In either case the coordinator drains its buffered hints for that node and replays them as replica sub-writes (`PUT`/`DELETE /keys/:key` with `X-Proxied-From` set). Hints that fail to deliver are re-buffered with their original TTL.

## Prometheus Metrics

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `continuum_http_requests_total` | Counter | Request count by method, path, status |
| `continuum_http_request_duration_seconds` | Histogram | Request latency by method and path |
| `continuum_ring_node_count` | Gauge | Current physical node count |
| `continuum_ring_vnode_count` | Gauge | Current virtual node count |
| `continuum_ring_key_lookups_total` | Counter | Total key lookups performed |
| `continuum_ring_distribution_variance` | Gauge | Key distribution variance across nodes |
| `continuum_ring_healthy_nodes` | Gauge | Nodes currently alive per gossip |
| `continuum_ring_suspect_nodes` | Gauge | Nodes currently suspect per gossip |
| `continuum_ring_dead_nodes` | Gauge | Nodes currently dead per gossip |

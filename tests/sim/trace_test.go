//go:build sim

package sim

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Trace conformance: record what a real cluster does, then make TLC replay it
// against specs/Replication.tla.
//
// The other direction of verification asks "does the model satisfy its
// invariants". This asks the question that actually connects the model to the
// code: **did the running system only ever do things the model permits?** If
// TLC cannot take the step an observation demands, the implementation left the
// behaviour the specification describes - and TLC names the exact step.
//
// The scenario is deliberately small and driven entirely from the test. Trace
// validation needs every event attributable, so a randomized background
// workload would only add events nothing can explain.
//
// Fidelity is bounded by what the harness can observe without instrumenting
// production internals. A store callback reveals *that* a replica now holds a
// version, not *which mechanism* delivered it - fan-out, hinted handoff, or
// anti-entropy all look identical from there. The trace therefore records the
// observation and lets the spec supply any action consistent with it (see
// DeliverStep in specs/Trace.tla). That is a weaker claim than naming the
// mechanism, and an honest one: the check is that *some* legal step explains
// what happened, not that we know which.
//
// Versions are resolved from entry hashes by order of first appearance rather
// than by reading the entry: the store fires onUpdate while holding its mutex,
// so reading it back from the callback deadlocks. The scenario is strictly
// sequential - the write settles before the delete is issued - so first
// appearance is the version order, and the test asserts exactly two distinct
// hashes to keep that assumption from failing silently.

const (
	traceKey = "trace-key"
	// The one value written, chosen so value -> version is a bijection: each
	// version is identifiable from the stored entry alone, without having to
	// map vector clocks onto the model's total order.
	traceValue = "v1"
	// Spec versions. The scenario writes once and deletes once, in that order,
	// matching the model's nextVer starting at 1.
	verWrite  = 1
	verDelete = 2
)

type traceEvent struct {
	Kind string // start | deliver | down | up | gc
	Node string
	Ver  int
	Val  string // spec value name, or "tombstone"
}

type traceRecorder struct {
	mu     sync.Mutex
	events []traceEvent
	// seen tracks the highest version observed per node, so repeated callbacks
	// for a version a replica already holds are not recorded as new steps.
	seen map[string]int
	// hashVer assigns a version to each distinct entry hash, by order of first
	// appearance. Sound only because the scenario is strictly sequential.
	hashVer map[uint32]int
	started bool
}

func newTraceRecorder() *traceRecorder {
	return &traceRecorder{seen: make(map[string]int), hashVer: make(map[uint32]int)}
}

func (r *traceRecorder) add(e traceEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

// observeUpdate fires when a node's store changes, under the store's own lock.
// It must not touch the store.
func (r *traceRecorder) observeUpdate(nodeID, key string, hash uint32) {
	if key != traceKey {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return
	}
	ver, ok := r.hashVer[hash]
	if !ok {
		ver = len(r.hashVer) + 1
		r.hashVer[hash] = ver
	}
	if r.seen[nodeID] >= ver {
		return // this replica already holds it
	}
	r.seen[nodeID] = ver
	r.events = append(r.events, traceEvent{Kind: "deliver", Node: nodeID, Ver: ver})
}

func (r *traceRecorder) observeEvict(nodeID, key string) {
	// Tombstone collection is recorded once, by the scenario driving it, rather
	// than once per node: the model collects across the cluster in one step.
	_ = nodeID
	_ = key
}

func (r *traceRecorder) begin() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = true
}

func (r *traceRecorder) snapshot() []traceEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]traceEvent(nil), r.events...)
}

// distinctVersions is the number of distinct entry hashes seen. The scenario
// produces exactly two - the write and the tombstone - and anything else means
// the version mapping below is not the one the trace assumes.
func (r *traceRecorder) distinctVersions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.hashVer)
}

// TestTraceConformance drives a cluster through a write, a crash, a delete, a
// recovery and a tombstone collection, records what every replica did, and
// writes the result as a TLA+ module for TLC to replay. `make spec-trace` runs
// this and then the checker.
func TestTraceConformance(t *testing.T) {
	rec := newTraceRecorder()
	c := newSimCluster(t, simConfig{
		nodes: 3, replicationFactor: 3, writeQuorum: 2, readQuorum: 2,
		observer: rec,
	}, 1001)
	c.waitFullRing(10 * time.Second)
	rec.begin()

	nodes := c.running()
	coordinator, victim := nodes[0], nodes[2]

	// 1. Write, acknowledged at quorum.
	rec.add(traceEvent{Kind: "start", Ver: verWrite, Val: traceValue})
	if code, err := c.put(coordinator, traceKey, traceValue, nil, false); err != nil || code != http.StatusNoContent {
		t.Fatalf("write got %d (err %v), want 204", code, err)
	}
	waitReplicas(t, c, traceKey, false, 2, 10*time.Second)

	// 2. A replica drops out, so the delete below reaches only part of the set.
	rec.add(traceEvent{Kind: "down", Node: victim.id})
	c.crash(victim)

	// 3. Delete, still quorate without the crashed replica.
	rec.add(traceEvent{Kind: "start", Ver: verDelete, Val: "tombstone"})
	if code, err := c.del(coordinator, traceKey, nil); err != nil || code != http.StatusNoContent {
		t.Fatalf("delete got %d (err %v), want 204", code, err)
	}
	waitReplicas(t, c, traceKey, true, 2, 10*time.Second)

	// 4. The crashed replica returns and hinted handoff carries the tombstone to
	//    it. This ordering is deliberate: hints expire after an hour while GC
	//    only collects tombstones older than a day, so a live hint can never
	//    outlive the tombstone it carries. Collecting first - which an earlier
	//    version of this scenario did - produces a trace the model rejects,
	//    because it is an ordering the real system cannot reach.
	rec.add(traceEvent{Kind: "up", Node: victim.id})
	c.restart(victim)
	c.waitFullRing(10 * time.Second)
	waitReplicas(t, c, traceKey, true, 3, 10*time.Second)

	// 5. Collect the tombstone on every replica. The background pass runs on a
	//    24h TTL, so the scenario drives it rather than waiting out a clock the
	//    harness has compressed away.
	for _, n := range c.running() {
		if _, err := n.store.GCTombstones(0); err != nil {
			t.Fatalf("%s: GC: %v", n.id, err)
		}
	}
	rec.add(traceEvent{Kind: "gc", Ver: verDelete})

	if got := rec.distinctVersions(); got != 2 {
		t.Fatalf("saw %d distinct entry hashes, want exactly 2 (write, tombstone) - "+
			"the version mapping this trace assumes does not hold", got)
	}

	events := rec.snapshot()
	if len(events) < 6 {
		t.Fatalf("recorded only %d events; the scenario did not exercise the path", len(events))
	}

	path := filepath.Join("..", "..", "specs", "TraceData.tla")
	if err := writeTraceModule(path, events, c.nodeIDs()); err != nil {
		t.Fatalf("write trace module: %v", err)
	}
	t.Logf("recorded %d events -> %s", len(events), path)
	for _, e := range events {
		t.Logf("  %s", describe(e))
	}
}

func describe(e traceEvent) string {
	switch e.Kind {
	case "start":
		return fmt.Sprintf("start   ver=%d val=%s", e.Ver, e.Val)
	case "deliver":
		return fmt.Sprintf("deliver %s ver=%d", e.Node, e.Ver)
	case "gc":
		return fmt.Sprintf("gc      ver=%d", e.Ver)
	default:
		return fmt.Sprintf("%-7s %s", e.Kind, e.Node)
	}
}

// holdsState reports whether a node's local store holds the key in the state
// the scenario is waiting for: the written value, or a tombstone.
func holdsState(n *simNode, key string, wantDeleted bool) bool {
	entry, ok, err := n.store.Get(key)
	if err != nil || !ok || len(entry.Siblings) == 0 {
		return false
	}
	sib := entry.Siblings[0]
	return sib.Deleted == wantDeleted && (wantDeleted || sib.Value == traceValue)
}

// waitReplicas waits until at least `want` replicas hold the key in the given
// state locally. Reading each store directly observes replication itself rather
// than a coordinator read that could mask a replica which never received it.
func waitReplicas(t *testing.T, c *simCluster, key string, wantDeleted bool, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := 0
		for _, n := range c.running() {
			if holdsState(n, key, wantDeleted) {
				got++
			}
		}
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d replicas reached the expected state (deleted=%v) within %v",
				got, want, wantDeleted, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// nodeIDs returns every node ID in the cluster, running or not, sorted.
func (c *simCluster) nodeIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		ids = append(ids, n.id)
	}
	sort.Strings(ids)
	return ids
}

// writeTraceModule emits the recorded run as a TLA+ module. It is generated per
// run rather than committed: the point is to check the implementation as it is
// now, not as it was when a fixture was recorded.
func writeTraceModule(path string, events []traceEvent, nodes []string) error {
	var b strings.Builder
	b.WriteString("---- MODULE TraceData ----\n")
	b.WriteString("(* GENERATED by TestTraceConformance in tests/sim - do not edit. *)\n\n")

	b.WriteString("TraceNodes == {")
	for i, n := range nodes {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(specNode(n))
	}
	b.WriteString("}\n\n")

	b.WriteString("TraceEvents == <<\n")
	for i, e := range events {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  " + record(e))
	}
	b.WriteString("\n>>\n\n====\n")
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func record(e traceEvent) string {
	node := "\"\""
	if e.Node != "" {
		node = specNode(e.Node)
	}
	val := "\"\""
	if e.Val != "" {
		val = fmt.Sprintf("%q", e.Val)
	}
	return fmt.Sprintf("[kind |-> %q, node |-> %s, ver |-> %d, val |-> %s]", e.Kind, node, e.Ver, val)
}

// specNode renders a node ID as a TLA+ model value. Sim IDs are already plain
// identifiers, so they need only quoting away from TLA+'s keyword space.
func specNode(id string) string {
	return fmt.Sprintf("%q", id)
}

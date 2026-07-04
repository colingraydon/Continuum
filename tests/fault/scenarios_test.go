//go:build fault

package fault

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Scenario 1: a replica is SIGKILLed under sustained load and later restarted.
// The crash restart trips the downtime gate (no clean shutdown ever happened
// on that node), so it comes back empty and must be refilled by anti-entropy,
// hinted handoff, and read repair. Invariants: every acknowledged write
// survives, and all replicas converge after heal.
func TestFault_ReplicaCrashRestartUnderLoad(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newWorkload(c, 4, 6, nil)
	w.run()

	time.Sleep(3 * time.Second)
	victim := c.nodes[1]
	t.Logf("killing %s under load", victim.id)
	c.kill(victim)

	time.Sleep(6 * time.Second) // writes must keep meeting W=2 on the survivors

	t.Logf("restarting %s", victim.id)
	c.restart(victim)
	time.Sleep(3 * time.Second)

	acked, failed := w.halt()
	t.Logf("workload: %d acked, %d failed", acked, failed)
	if acked == 0 {
		t.Fatal("workload never acknowledged a write; harness is broken")
	}

	verifyDurability(t, c, w, 30*time.Second)
	verifyConvergence(t, c, w.keyNames(), 45*time.Second)
}

// Scenario 2: single-node storage faults. A clean shutdown stamps meta.json;
// after that, a SIGKILL mid-stream must recover everything from SSTables plus
// the WAL tail. The tiny memtable threshold forces real flushes so recovery
// exercises tables, manifest, and WAL replay together.
//
// Key layout after the fault sequence: 0-4 deleted, 10-19 overwritten with
// v2, everything else holds its original v1.
func TestFault_CrashRecoveryFromWALAndTables(t *testing.T) {
	c := newCluster(t, clusterConfig{
		nodes: 1, replicationFactor: 1, writeQuorum: 1, readQuorum: 1,
		memtableMaxBytes: 2048,
	})
	n := c.nodes[0]

	putCrashKeys(t, c, n, 0, 30, "v1")
	for i := 0; i < 5; i++ {
		mustDelete(t, c, n, crashKey(i))
	}

	c.shutdown(n) // clean: flush + meta.json
	c.restart(n)

	// Second generation of writes lives only in tables + WAL tail.
	putCrashKeys(t, c, n, 30, 60, "v1")
	putCrashKeys(t, c, n, 10, 20, "v2")

	c.kill(n) // hard crash: recovery must come from disk alone
	c.restart(n)

	for i := 0; i < 60; i++ {
		assertCrashKeyRecovered(t, c, n, i)
	}
}

// seqKeys names a batch of sequential test writes: key i is "<keyPrefix>-k%02d"
// holding value "<valPrefix>-%02d". Shared by the hinted-handoff scenarios,
// which all write a run of keys through a coordinator and later assert they
// reached a replica's local store.
type seqKeys struct{ keyPrefix, valPrefix string }

func (s seqKeys) key(i int) string { return fmt.Sprintf("%s-k%02d", s.keyPrefix, i) }
func (s seqKeys) val(i int) string { return fmt.Sprintf("%s-%02d", s.valPrefix, i) }

// putSeq writes n sequential key/value pairs through coordinator via the
// unfaulted side channel, asserting each is acknowledged with 204.
func putSeq(t *testing.T, c *cluster, coordinator *node, sk seqKeys, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		code, err := c.put(coordinator, sk.key(i), sk.val(i), nil)
		if err != nil || code != http.StatusNoContent {
			t.Fatalf("put %s: code=%d err=%v (want 204 via the reachable replicas)", sk.key(i), code, err)
		}
	}
}

// waitLocalValues polls target's local store until every one of the n keys
// holds its expected value or timeout elapses, returning how many are still
// missing (0 means fully converged).
func (c *cluster) waitLocalValues(target *node, sk seqKeys, n int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	missing := n
	for time.Now().Before(deadline) && missing > 0 {
		missing = 0
		for i := 0; i < n; i++ {
			nr, code, err := c.directGet(target, sk.key(i))
			if err != nil || code != http.StatusOK || nr.Value != sk.val(i) {
				missing++
			}
		}
		if missing > 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return missing
}

// keyReplicatedOn probes for a key whose strict replica set includes every
// required node, returning the key and its full strict set.
func (c *cluster) keyReplicatedOn(t *testing.T, required ...*node) (string, []*node) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("sloppy-k%03d", i)
		set := c.replicaSet(key)
		if containsAllNodes(set, required) {
			return key, set
		}
	}
	t.Fatal("no key found whose replica set includes all required nodes")
	return "", nil
}

func containsAllNodes(set, required []*node) bool {
	ids := make(map[string]bool, len(set))
	for _, n := range set {
		ids[n.id] = true
	}
	for _, r := range required {
		if !ids[r.id] {
			return false
		}
	}
	return true
}

// nodeOutside returns a cluster node that is not in set.
func (c *cluster) nodeOutside(t *testing.T, set []*node) *node {
	t.Helper()
	ids := make(map[string]bool, len(set))
	for _, n := range set {
		ids[n.id] = true
	}
	for _, n := range c.nodes {
		if !ids[n.id] {
			return n
		}
	}
	t.Fatal("test setup: expected a node outside the given replica set")
	return nil
}

// waitLocalValue polls target's local store until key holds want or timeout
// elapses, reporting whether it converged.
func (c *cluster) waitLocalValue(target *node, key, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nr, code, err := c.directGet(target, key); err == nil && code == http.StatusOK && nr.Value == want {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func crashKey(i int) string { return fmt.Sprintf("crash-k%02d", i) }

func putCrashKeys(t *testing.T, c *cluster, n *node, from, to int, gen string) {
	t.Helper()
	for i := from; i < to; i++ {
		mustPut(t, c, n, crashKey(i), fmt.Sprintf("%s-%02d", gen, i))
	}
}

// assertCrashKeyRecovered checks one key against the layout documented on
// TestFault_CrashRecoveryFromWALAndTables.
func assertCrashKeyRecovered(t *testing.T, c *cluster, n *node, i int) {
	t.Helper()
	key := crashKey(i)
	nr, code, err := c.get(n, key)
	if err != nil {
		t.Fatalf("get %s after crash: %v", key, err)
	}
	if i < 5 {
		if code != http.StatusNotFound {
			t.Errorf("%s: deleted key returned code=%d value=%q, want 404", key, code, nr.Value)
		}
		return
	}
	want := fmt.Sprintf("v1-%02d", i)
	if i >= 10 && i < 20 {
		want = fmt.Sprintf("v2-%02d", i)
	}
	if nr.Value != want {
		t.Errorf("%s: got %q, want %q", key, nr.Value, want)
	}
}

// Scenario 3: a hung (SIGSTOPped) replica. Unlike a killed node, connections
// are accepted by the kernel and time out instead of being refused, so the
// replica timeout path and hinted handoff on timeout are exercised. The node
// is resumed before the dead threshold, so it transitions suspect -> alive,
// which must trigger hint delivery.
func TestFault_HungReplicaTimeoutsAndRecovery(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newWorkload(c, 4, 6, nil)
	w.run()

	time.Sleep(2 * time.Second)
	victim := c.nodes[2]
	t.Logf("pausing %s (hang, not crash)", victim.id)
	c.pause(victim)

	time.Sleep(8 * time.Second) // suspect after ~5s, but resumed before dead (~10s)

	t.Logf("resuming %s", victim.id)
	c.resume(victim)
	time.Sleep(4 * time.Second)

	acked, failed := w.halt()
	t.Logf("workload: %d acked, %d failed", acked, failed)

	c.waitFullRing(30 * time.Second)
	verifyDurability(t, c, w, 30*time.Second)
	verifyConvergence(t, c, w.keyNames(), 45*time.Second)
}

// Scenario 4: asymmetric partition. All inbound traffic to node1 is
// blackholed while its outbound still flows: peers keep seeing its gossip
// (it stays alive in their view and their writes to it buffer hints), while
// node1 sees silence, declares the others dead, shrinks its ring to itself,
// and starts acknowledging single-copy writes with a clamped quorum. After
// heal, membership self-repairs via gossip heartbeats and both sides'
// writes must converge everywhere.
func TestFault_AsymmetricPartitionHeals(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	w := newWorkload(c, 4, 6, nil)
	w.run()

	time.Sleep(2 * time.Second)
	isolated := c.nodes[0]
	t.Logf("isolating %s (inbound blackhole; outbound stays open)", isolated.id)
	c.isolate(isolated)

	time.Sleep(14 * time.Second)

	t.Logf("healing %s", isolated.id)
	c.heal(isolated)
	time.Sleep(4 * time.Second)

	acked, failed := w.halt()
	t.Logf("workload: %d acked, %d failed", acked, failed)

	c.waitFullRing(60 * time.Second)
	verifyDurability(t, c, w, 45*time.Second)
	verifyConvergence(t, c, w.keyNames(), 60*time.Second)
}

// Scenario 5: the persistent hint log. A replica is unreachable over HTTP
// (gossip stays up so it is never declared dead and stays in the replica
// set), the coordinator buffers hints for it, and then the coordinator is
// SIGKILLed. Anti-entropy is effectively disabled, so the ONLY path that can
// repair the replica is the coordinator replaying its hint log after
// restart. The coordinator's own store data is wiped by the downtime gate,
// which proves the hints (which carry full values) were themselves durable.
func TestFault_HintLogSurvivesCoordinatorCrash(t *testing.T) {
	c := newCluster(t, clusterConfig{syncIntervalMS: 600_000})
	coordinator, replica := c.nodes[0], c.nodes[2]

	t.Logf("blackholing HTTP to %s (gossip stays open)", replica.id)
	replica.httpProxy.Blackhole()

	const keys = 10
	sk := seqKeys{"hint", "hinted"}
	putSeq(t, c, coordinator, sk, keys)
	time.Sleep(1500 * time.Millisecond) // async hint buffering + fsync of the hint log

	t.Logf("crashing coordinator %s with buffered hints", coordinator.id)
	c.kill(coordinator)
	replica.httpProxy.Heal()

	t.Logf("restarting %s; hint log replay + alive-transition delivery must repair %s", coordinator.id, replica.id)
	c.restart(coordinator)

	if missing := c.waitLocalValues(replica, sk, keys, 25*time.Second); missing > 0 {
		t.Errorf("%d/%d hinted values never reached %s via hint-log replay", missing, keys, replica.id)
	}
}

// Scenario 6: graceful decommission. Keys seeded onto exactly one node must
// be pushed to the surviving replicas during SIGTERM shutdown
// (push-on-leave). Anti-entropy is effectively disabled so the push path is
// the only possible mover.
func TestFault_DecommissionPushesKeysToSuccessors(t *testing.T) {
	c := newCluster(t, clusterConfig{syncIntervalMS: 600_000})
	leaving := c.nodes[2]

	const keys = 10
	for i := 0; i < keys; i++ {
		c.putReplica(leaving, fmt.Sprintf("leave-k%02d", i), fmt.Sprintf("moved-%02d", i), map[string]uint64{"seeder": uint64(i + 1)})
	}

	t.Logf("gracefully shutting down %s; push-on-leave must move its keys", leaving.id)
	c.shutdown(leaving)

	for _, survivor := range []*node{c.nodes[0], c.nodes[1]} {
		for i := 0; i < keys; i++ {
			key := fmt.Sprintf("leave-k%02d", i)
			nr, code, err := c.directGet(survivor, key)
			if err != nil || code != http.StatusOK || nr.Value != fmt.Sprintf("moved-%02d", i) {
				t.Errorf("%s missing pushed key %s: code=%d value=%q err=%v", survivor.id, key, code, nr.Value, err)
			}
		}
	}
}

// Scenario 7: quorum loss and the quorum-clamping semantic. With two of
// three nodes dead, writes 503 while gossip still believes them alive
// (fan-out fails, acks < W). Once the suspect verdict lands, the sloppy
// healthy walk excludes them and the write quorum clamps to the surviving
// replica set (min(W, len(healthy))) and single-copy writes are acknowledged.
// This test documents that behavior: W is not a hard durability floor under
// membership churn. After the nodes return, everything must converge.
func TestFault_QuorumLossThenClampedAvailability(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	survivor := c.nodes[0]

	if code, err := c.put(survivor, "quorum-k", "before", nil); err != nil || code != http.StatusNoContent {
		t.Fatalf("healthy write: code=%d err=%v", code, err)
	}

	t.Log("killing 2 of 3 nodes")
	c.kill(c.nodes[1])
	c.kill(c.nodes[2])

	// While gossip still believes the dead nodes alive, the healthy walk
	// includes them, their connections are refused, and quorum cannot be met.
	code, err := c.put(survivor, "quorum-k", "during-outage", nil)
	if err == nil && code == http.StatusNoContent {
		t.Errorf("write acknowledged immediately after killing 2/3 nodes; expected quorum failure, got %d", code)
	}

	// Once the suspect verdict lands, the sloppy walk excludes the dead nodes
	// and the quorum clamps to the survivor: writes are accepted again
	// (single copy). Before sloppy quorum this waited for the dead verdict to
	// remove them from the ring.
	deadline := time.Now().Add(30 * time.Second)
	clamped := false
	for time.Now().Before(deadline) {
		code, err := c.put(survivor, "quorum-k", "clamped", nil)
		if err == nil && code == http.StatusNoContent {
			clamped = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !clamped {
		t.Fatal("writes never resumed after ring dropped dead members (quorum clamp)")
	}

	t.Log("restarting the dead nodes")
	c.restart(c.nodes[1])
	c.restart(c.nodes[2])

	verifyConvergence(t, c, []string{"quorum-k"}, 45*time.Second)

	nr, code, err := c.get(survivor, "quorum-k")
	if err != nil || code != http.StatusOK || nr.Value != "clamped" {
		t.Errorf("final read: code=%d value=%q err=%v, want the clamped write", code, nr.Value, err)
	}
}

// Scenario 9: periodic hint delivery closes the asymmetric-partition blind
// spot. A replica's inbound HTTP is blackholed while its gossip stays up, so it
// is never declared dead and never presents a dead->alive transition - the
// edge the event-driven delivery callback keys on. The coordinator is NOT
// restarted (so no restart-triggered delivery) and anti-entropy is disabled.
// The only path that can repair the replica is the periodic delivery sweep
// (runHintDelivery), which delivers buffered hints to any alive target on a
// timer. Re-buffering of failed deliveries is exercised implicitly: several
// sweeps fire and fail against the still-blackholed replica before the heal,
// and the hints must survive all of them.
func TestFault_PeriodicHintDeliveryAcrossAsymmetricPartition(t *testing.T) {
	c := newCluster(t, clusterConfig{syncIntervalMS: 600_000, hintDeliveryMS: 1000})
	coordinator, replica := c.nodes[0], c.nodes[2]

	t.Logf("blackholing HTTP to %s (gossip stays open, so it never looks dead)", replica.id)
	replica.httpProxy.Blackhole()

	const keys = 10
	sk := seqKeys{"psweep", "swept"}
	putSeq(t, c, coordinator, sk, keys)

	// Let several delivery sweeps (1s interval) fire and fail against the still-
	// blackholed replica. The hints must be re-buffered each time, not dropped.
	time.Sleep(4 * time.Second)

	// The replica must never have left the coordinator's ring: if it had gone
	// dead and come back, an alive-transition would deliver hints and this test
	// would no longer isolate the periodic sweep as the cause.
	if ids := c.ringIDs(coordinator); !ids[replica.id] {
		t.Fatalf("%s dropped from %s's ring; test can no longer isolate the periodic sweep (ring=%v)", replica.id, coordinator.id, ids)
	}
	// Nothing should have reached the blackholed replica yet. A local read of a
	// missing key returns 200 with an empty value, so a leak is a matching value.
	for i := 0; i < keys; i++ {
		if nr, _, _ := c.directGet(replica, sk.key(i)); nr.Value == sk.val(i) {
			t.Fatalf("%s reached %s while still blackholed (value=%q); blackhole leaked", sk.key(i), replica.id, nr.Value)
		}
	}

	t.Logf("healing HTTP to %s; only the periodic sweep can now deliver (no dead->alive edge, AE off)", replica.id)
	replica.httpProxy.Heal()

	if missing := c.waitLocalValues(replica, sk, keys, 20*time.Second); missing > 0 {
		t.Errorf("%d/%d hinted values never reached %s via the periodic delivery sweep", missing, keys, replica.id)
	}
}

// Scenario 10: sloppy quorum. A 4-node cluster with RF=3: one strict-set
// replica of a probed key is killed, and once gossip marks it suspect the
// health-aware replica walk must skip it and pull in the 4th node as a
// substitute, so a consistency=all write (3 acks) succeeds where a strict
// quorum would 503 - Dynamo's "always writable" property. The skipped owner
// gets a hint; after the victim restarts, hint delivery (anti-entropy is off)
// must land the value on the intended owner. The per-request ?consistency=all
// override is exercised end-to-end in the same flow.
func TestFault_SloppyQuorumAlwaysWritable(t *testing.T) {
	c := newCluster(t, clusterConfig{nodes: 4, replicationFactor: 3, syncIntervalMS: 600_000})
	coordinator := c.nodes[0]
	victim := c.nodes[2]

	// A key replicated on the victim (so the walk must skip it) and the
	// coordinator (so self's ack cannot mask the victim's failure: acks stay
	// at 2 of 3 until a substitute fills in). The substitute the sloppy walk
	// must fall through to is the one node outside the strict set.
	key, strict := c.keyReplicatedOn(t, victim, coordinator)
	substitute := c.nodeOutside(t, strict)

	t.Logf("killing strict replica %s (key %s, substitute %s)", victim.id, key, substitute.id)
	c.kill(victim)

	// While gossip still believes the victim is alive, the walk includes it,
	// its connection is refused, and consistency=all cannot be met.
	if code, err := c.putConsistency(coordinator, key, "v-strict", "all"); err != nil || code != http.StatusServiceUnavailable {
		t.Fatalf("consistency=all with an in-view dead replica: code=%d err=%v, want 503", code, err)
	}

	// Wait for the suspect verdict, then write inside the suspect window
	// (before the dead verdict removes the victim from the ring entirely).
	c.waitMemberStatus(t, coordinator, victim.id, "suspect", 10*time.Second)
	if code, err := c.putConsistency(coordinator, key, "v-sloppy", "all"); err != nil || code != http.StatusNoContent {
		t.Fatalf("sloppy consistency=all write: code=%d err=%v, want 204 via substitute", code, err)
	}

	// The substitute must hold the value (it was needed for the 3rd ack).
	if nr, code, err := c.directGet(substitute, key); err != nil || code != http.StatusOK || nr.Value != "v-sloppy" {
		t.Errorf("substitute %s: code=%d value=%q err=%v, want v-sloppy", substitute.id, code, nr.Value, err)
	}

	// Restart the intended owner: with anti-entropy off, only hint delivery
	// can land the value there.
	t.Logf("restarting %s; hint replay must repair the intended owner", victim.id)
	c.restart(victim)
	if !c.waitLocalValue(victim, key, "v-sloppy", 25*time.Second) {
		nr, code, err := c.directGet(victim, key)
		t.Fatalf("hinted value never reached intended owner %s: code=%d value=%q err=%v", victim.id, code, nr.Value, err)
	}
}

// Scenario 8: lossy gossip. With 40% of gossip datagrams dropped, membership
// must stay stable (gossip is designed to tolerate loss: fanout 3, 1s
// interval, 5s suspect threshold) and the data path must be unaffected.
func TestFault_GossipPacketLossStability(t *testing.T) {
	c := newCluster(t, clusterConfig{})
	for _, n := range c.nodes {
		n.udpProxy.SetDropPermille(400)
	}

	w := newWorkload(c, 3, 5, nil)
	w.run()

	// Sample membership while the workload runs: no node may ever drop out of
	// any ring (suspect is fine; dead/removal is a false positive).
	for i := 0; i < 12; i++ {
		time.Sleep(1 * time.Second)
		for _, n := range c.nodes {
			ids := c.ringIDs(n)
			if len(ids) < len(c.nodes) {
				t.Errorf("t=%ds: %s ring shrank to %v under 40%% gossip loss (false failure detection)", i+1, n.id, ids)
			}
		}
	}

	acked, failed := w.halt()
	t.Logf("workload under 40%% gossip loss: %d acked, %d failed", acked, failed)
	if acked == 0 {
		t.Fatal("no writes succeeded under gossip packet loss")
	}

	for _, n := range c.nodes {
		n.udpProxy.Heal()
	}
	verifyDurability(t, c, w, 30*time.Second)
	verifyConvergence(t, c, w.keyNames(), 45*time.Second)
}

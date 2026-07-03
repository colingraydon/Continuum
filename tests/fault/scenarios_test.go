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
	for i := 0; i < keys; i++ {
		code, err := c.put(coordinator, fmt.Sprintf("hint-k%02d", i), fmt.Sprintf("hinted-%02d", i), nil)
		if err != nil || code != http.StatusNoContent {
			t.Fatalf("put %d: code=%d err=%v (want 204 via the two reachable replicas)", i, code, err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // async hint buffering + fsync of the hint log

	t.Logf("crashing coordinator %s with buffered hints", coordinator.id)
	c.kill(coordinator)
	replica.httpProxy.Heal()

	t.Logf("restarting %s; hint log replay + alive-transition delivery must repair %s", coordinator.id, replica.id)
	c.restart(coordinator)

	deadline := time.Now().Add(25 * time.Second)
	missing := keys
	for time.Now().Before(deadline) && missing > 0 {
		missing = 0
		for i := 0; i < keys; i++ {
			nr, code, err := c.directGet(replica, fmt.Sprintf("hint-k%02d", i))
			if err != nil || code != http.StatusOK || nr.Value != fmt.Sprintf("hinted-%02d", i) {
				missing++
			}
		}
		if missing > 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if missing > 0 {
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
// three nodes dead, writes 503 while the dead nodes are still in the ring
// (fan-out fails, acks < W). Once gossip declares them dead and the ring
// shrinks, the write quorum clamps to the surviving replica set
// (min(W, len(replicas))) and single-copy writes are acknowledged. This test
// documents that behavior: W is not a hard durability floor under
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

	// While the dead nodes are still ring members, quorum cannot be met.
	code, err := c.put(survivor, "quorum-k", "during-outage", nil)
	if err == nil && code == http.StatusNoContent {
		t.Errorf("write acknowledged immediately after killing 2/3 nodes; expected quorum failure, got %d", code)
	}

	// After the stale threshold the ring drops the dead nodes and the quorum
	// clamps to the survivor: writes are accepted again (single copy).
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

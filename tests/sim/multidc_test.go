//go:build sim

package sim

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// The two-DC topology every scenario in this file runs on: three nodes in each
// data center, a full replica set in each. The cluster RF is therefore 6 and a
// cluster-wide quorum needs 4 acks spanning both DCs, while a local quorum
// needs 2 from one side — the asymmetry these tests exist to demonstrate.
const (
	dcEast = "us-east"
	dcWest = "eu-west"
)

func newMultiDCCluster(t *testing.T, seed int64) *simCluster {
	t.Helper()
	c := newSimCluster(t, simConfig{
		nodes:         6,
		dcs:           []string{dcEast, dcEast, dcEast, dcWest, dcWest, dcWest},
		dcReplication: map[string]int{dcEast: 3, dcWest: 3},
	}, seed)
	c.waitDCsPropagated(10 * time.Second)
	return c
}

// TestSimMultiDCPlacement asserts the topology the other scenarios depend on:
// with a per-DC table every key keeps a full replica set in each DC. A
// regression here would make the partition scenarios below pass for the wrong
// reason.
func TestSimMultiDCPlacement(t *testing.T) {
	c := newMultiDCCluster(t, 900)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("place-%d", i)
		perDC := make(map[string]int)
		for _, n := range c.replicaSet(key) {
			perDC[n.dc]++
		}
		if perDC[dcEast] != 3 || perDC[dcWest] != 3 {
			t.Fatalf("%s: replicas per DC = %v, want 3 in each", key, perDC)
		}
	}
}

// TestSimCrossDCPartition_LocalQuorumStaysAvailable is the scenario the whole
// multi-DC arc is for: the WAN link between the two data centers is cut, both
// sides stay internally quorate, and a client talking to its local DC keeps
// working while a cluster-wide quorum cannot be reached.
func TestSimCrossDCPartition_LocalQuorumStaysAvailable(t *testing.T) {
	c := newMultiDCCluster(t, 901)
	east := c.nodesInDC(dcEast)[0]

	// Arrange: cut the WAN. Each DC still holds a full replica set, so this is
	// precisely the failure per-DC placement was designed to absorb.
	c.partitionDCs(dcEast, dcWest)
	t.Cleanup(c.net.healAll)

	// Act + Assert: local_quorum needs 2 of the 3 us-east replicas, all of
	// which are reachable from east.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("split-%d", i)
		code, err := c.putConsistency(east, key, fmt.Sprintf("v%d", i), "local_quorum")
		if err != nil {
			t.Fatalf("%s: local_quorum write errored: %v", key, err)
		}
		if code != http.StatusNoContent {
			t.Fatalf("%s: local_quorum write got %d, want 204 across a WAN cut", key, code)
		}
	}

	// Assert: the same writes at cluster-wide quorum cannot reach 4 of 6.
	code, err := c.putConsistency(east, "split-cluster", "v", "quorum")
	if err != nil {
		t.Fatalf("cluster quorum write errored: %v", err)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("cluster quorum write got %d, want 503 across a WAN cut", code)
	}

	// Assert: reads behave the same way round.
	if _, code, err := c.getConsistency(east, "split-0", "local_quorum"); err != nil || code != http.StatusOK {
		t.Errorf("local_quorum read got %d (err %v), want 200", code, err)
	}
	if _, code, err := c.getConsistency(east, "split-0", "quorum"); err == nil && code != http.StatusServiceUnavailable {
		t.Errorf("cluster quorum read got %d, want 503 across a WAN cut", code)
	}
}

// TestSimCrossDCPartition_WritesReachRemoteDCAfterHeal closes the durability
// half of the story. Accepting a write on local quorum alone is only safe if
// the remote DC eventually receives it, so the partition is healed and every
// key written during the cut must appear on the far side — carried by hinted
// handoff and anti-entropy, with no further client traffic.
func TestSimCrossDCPartition_WritesReachRemoteDCAfterHeal(t *testing.T) {
	c := newMultiDCCluster(t, 902)
	east := c.nodesInDC(dcEast)[0]

	// Arrange: write under a WAN cut at local quorum.
	c.partitionDCs(dcEast, dcWest)
	keys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("heal-%d", i)
		code, err := c.putConsistency(east, key, fmt.Sprintf("v%d", i), "local_quorum")
		if err != nil || code != http.StatusNoContent {
			t.Fatalf("%s: local_quorum write got %d (err %v), want 204", key, code, err)
		}
		keys = append(keys, key)
	}

	// Assert the precondition, so a pass here cannot be vacuous: while the WAN
	// is cut the remote DC must hold none of these writes. Without this, a
	// harness bug that let traffic cross the partition would make the
	// post-heal assertion below succeed for the wrong reason.
	if missing := missingOnDC(c, keys, dcWest); len(missing) != len(keys) {
		t.Fatalf("%d of %d keys reached %s while partitioned; the WAN cut is not effective",
			len(keys)-len(missing), len(keys), dcWest)
	}

	// Act: restore the WAN and let repair run.
	c.net.healAll()
	c.waitFullRing(10 * time.Second)

	// Assert: every write survives and both DCs converge on it.
	verifyConvergence(t, c, keys, 15*time.Second)
	waitKeysOnDC(t, c, keys, dcWest, 15*time.Second)
}

// waitKeysOnDC waits until at least one node in dc holds each key in its local
// store. It reads the store directly rather than through the API so it observes
// replication itself, not a coordinator fan-out that could mask a DC that never
// received the data.
func waitKeysOnDC(t *testing.T, c *simCluster, keys []string, dc string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		missing := missingOnDC(c, keys, dc)
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d keys never reached %s within %v (e.g. %v)", len(missing), dc, timeout, missing[:min(3, len(missing))])
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// missingOnDC returns the keys no node in dc holds locally.
func missingOnDC(c *simCluster, keys []string, dc string) []string {
	var missing []string
	for _, key := range keys {
		found := false
		for _, n := range c.nodesInDC(dc) {
			if _, ok, err := n.store.Get(key); err == nil && ok {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, key)
		}
	}
	return missing
}

// TestSimCrossDCPartition_LocalOneServesFromEitherSide checks the weaker level
// from both directions at once: with the WAN cut, a coordinator in either DC
// can still satisfy local_one, so neither side is the privileged one.
func TestSimCrossDCPartition_LocalOneServesFromEitherSide(t *testing.T) {
	c := newMultiDCCluster(t, 903)
	east, west := c.nodesInDC(dcEast)[0], c.nodesInDC(dcWest)[0]

	c.partitionDCs(dcEast, dcWest)
	t.Cleanup(c.net.healAll)

	for _, tc := range []struct {
		name string
		node *simNode
	}{{"east", east}, {"west", west}} {
		code, err := c.putConsistency(tc.node, "localone-"+tc.name, "v", "local_one")
		if err != nil {
			t.Errorf("%s: local_one write errored: %v", tc.name, err)
			continue
		}
		if code != http.StatusNoContent {
			t.Errorf("%s: local_one write got %d, want 204 across a WAN cut", tc.name, code)
		}
	}
}

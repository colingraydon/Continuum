//go:build fault

package fault

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// The two-DC topology these scenarios run on: three real processes in each
// data center, a full replica set in each. The cluster RF is 6, so a
// cluster-wide quorum needs 4 acks spanning both DCs while a local quorum
// needs 2 from one side.
const (
	dcEast = "us-east"
	dcWest = "eu-west"
)

func newMultiDCCluster(t *testing.T) *cluster {
	t.Helper()
	c := newCluster(t, clusterConfig{
		nodes:         6,
		dcs:           []string{dcEast, dcEast, dcEast, dcWest, dcWest, dcWest},
		dcReplication: map[string]int{dcEast: 3, dcWest: 3},
	})
	c.waitDCsPropagated(t, 30*time.Second)
	return c
}

// nodesInDC returns the cluster's nodes labeled dc, running or not.
func (c *cluster) nodesInDC(dc string) []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.dc == dc {
			out = append(out, n)
		}
	}
	return out
}

// isolateDC blackholes every node in dc, making the whole data center
// unreachable from the rest of the cluster and from the harness.
func (c *cluster) isolateDC(dc string) {
	for _, n := range c.nodesInDC(dc) {
		c.isolate(n)
	}
}

// healDC restores every node in dc.
func (c *cluster) healDC(dc string) {
	for _, n := range c.nodesInDC(dc) {
		c.heal(n)
	}
}

// waitDCsPropagated waits until every alive node's /stats reports a DC for
// every node it knows. Placement only becomes DC-aware once the labels arrive
// through gossip, so a scenario that cuts a DC before then would be asserting
// against a half-formed topology.
func (c *cluster) waitDCsPropagated(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if c.dcsPropagatedOnce() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DC labels did not propagate within %v", timeout)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// dcsPropagatedOnce reports whether every alive node sees a non-empty DC for
// all cfg.nodes peers.
func (c *cluster) dcsPropagatedOnce() bool {
	for _, n := range c.alive() {
		stats, err := c.ringStats(n)
		if err != nil || len(stats) != c.cfg.nodes {
			return false
		}
		for _, s := range stats {
			if s.DC == "" {
				return false
			}
		}
	}
	return true
}

// ringStats reads the per-node distribution from /stats, which carries each
// node's DC label.
func (c *cluster) ringStats(n *node) ([]struct {
	NodeID string `json:"node_id"`
	DC     string `json:"dc"`
}, error) {
	resp, err := c.client.Get(n.baseURL() + "/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var body struct {
		Distribution []struct {
			NodeID string `json:"node_id"`
			DC     string `json:"dc"`
		} `json:"distribution"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Distribution, nil
}

// Scenario: a whole data center goes dark — the multi-DC equivalent of the
// single-node isolation scenarios. Three real processes in eu-west are
// blackholed while load continues against us-east. local_quorum must keep
// serving from the surviving DC, a cluster-wide quorum must not, and every
// write accepted during the outage must reach eu-west once it returns.
func TestFault_RemoteDCOutage_LocalQuorumStaysAvailable(t *testing.T) {
	c := newMultiDCCluster(t)
	east := c.nodesInDC(dcEast)[0]

	// Arrange: take eu-west down entirely.
	t.Logf("isolating all of %s", dcWest)
	c.isolateDC(dcWest)
	t.Cleanup(func() { c.healDC(dcWest) })

	// Give gossip time to mark the remote DC's nodes suspect/dead so the
	// healthy walk reflects the outage rather than racing it.
	time.Sleep(5 * time.Second)

	// Act + Assert: local_quorum needs 2 of the 3 us-east replicas.
	var keys []string
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("dcout-%d", i)
		code, err := c.putConsistency(east, key, fmt.Sprintf("v%d", i), "local_quorum")
		if err != nil {
			t.Fatalf("%s: local_quorum write errored: %v", key, err)
		}
		if code != http.StatusNoContent {
			t.Fatalf("%s: local_quorum write got %d, want 204 with %s down", key, code, dcWest)
		}
		keys = append(keys, key)
	}

	// Assert: the same write at cluster-wide quorum cannot reach 4 of 6.
	code, err := c.putConsistency(east, "dcout-cluster", "v", "quorum")
	if err != nil {
		t.Fatalf("cluster quorum write errored: %v", err)
	}
	if code != http.StatusServiceUnavailable {
		t.Errorf("cluster quorum write got %d, want 503 with %s down", code, dcWest)
	}

	// Assert the precondition, so a pass here cannot be vacuous: while eu-west
	// is blackholed it must hold none of these writes. The harness reaches each
	// node on its real bind port, bypassing the proxy that isolates it from
	// peers, so this observes the DC's own store rather than a coordinator's
	// merged view. Without this check, a leaky isolation would make the
	// post-heal convergence assertion pass for the wrong reason.
	if reached := keysPresentOnDC(c, keys, dcWest); reached != 0 {
		t.Fatalf("%d of %d keys reached %s while it was isolated; the outage is not effective",
			reached, len(keys), dcWest)
	}

	// Act: bring the data center back.
	t.Logf("healing %s", dcWest)
	c.healDC(dcWest)
	c.waitFullRing(60 * time.Second)

	// Assert: nothing accepted on local quorum alone was lost — hinted handoff
	// and anti-entropy must carry it across once the WAN returns.
	//
	// Check the remote DC explicitly rather than relying on verifyConvergence
	// alone. Convergence resolves the replica set from a live node's ring, and
	// gossip evicts a DC it has declared dead, so with eu-west still absent the
	// check would collapse to the surviving DC and pass without proving
	// anything about cross-DC delivery.
	waitKeysOnDC(t, c, keys, dcWest, 90*time.Second)
	verifyConvergence(t, c, keys, 90*time.Second)
}

// waitKeysOnDC waits until every key is held by some node in dc.
func waitKeysOnDC(t *testing.T, c *cluster, keys []string, dc string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		present := keysPresentOnDC(c, keys, dc)
		if present == len(keys) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d keys reached %s within %v", present, len(keys), dc, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// keysPresentOnDC counts how many of keys any node in dc holds in its own
// store, read directly (X-Proxied-From) so no coordinator fan-out is involved.
//
// A replica read answers 200 with an empty body for a key the node does not
// hold — it never 404s — so presence is judged by the entry's contents, not by
// the status code.
func keysPresentOnDC(c *cluster, keys []string, dc string) int {
	present := 0
	for _, key := range keys {
		for _, n := range c.nodesInDC(dc) {
			nr, code, err := c.directGet(n, key)
			if err == nil && code == http.StatusOK && entryHeld(nr) {
				present++
				break
			}
		}
	}
	return present
}

// entryHeld reports whether a replica read actually returned a stored entry.
// Every stored sibling carries a version clock, so their absence means the
// node holds nothing for this key.
func entryHeld(nr nodeResponse) bool {
	return len(nr.Clocks) > 0 || len(nr.Siblings) > 0
}

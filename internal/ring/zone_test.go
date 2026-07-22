package ring

import (
	"fmt"
	"testing"
)

// newZonedRing builds a ring with two nodes in each of the given zones.
// Node IDs are "<zone>-1" and "<zone>-2".
func newZonedRing(t *testing.T, zones ...string) *Ring {
	t.Helper()
	r := NewRing(150)
	for i, z := range zones {
		r.AddZonedNode(fmt.Sprintf("%s-1", z), fmt.Sprintf("10.0.%d.1", i), z, 1.0)
		r.AddZonedNode(fmt.Sprintf("%s-2", z), fmt.Sprintf("10.0.%d.2", i), z, 1.0)
	}
	return r
}

func zonesOf(nodes []*Node) map[string]int {
	counts := make(map[string]int)
	for _, n := range nodes {
		counts[n.Zone]++
	}
	return counts
}

func TestAddZonedNode_SetsZone(t *testing.T) {
	// Arrange
	r := NewRing(150)

	// Act
	r.AddZonedNode("node1", "10.0.0.1", "rack1", 1.0)

	// Assert
	nodes := r.GetNodes()
	if len(nodes) != 1 || nodes[0].Zone != "rack1" {
		t.Fatalf("expected one node in zone rack1, got %+v", nodes)
	}
}

func TestAddZonedNodeDC_SetsDC(t *testing.T) {
	// Arrange
	r := NewRing(150)

	// Act
	r.AddZonedNodeDC("node1", "10.0.0.1", "us-east", "rack1", 1.0)

	// Assert: both failure-domain labels are recorded on the node.
	nodes := r.GetNodes()
	if len(nodes) != 1 || nodes[0].DC != "us-east" || nodes[0].Zone != "rack1" {
		t.Fatalf("expected one node in dc us-east / zone rack1, got %+v", nodes)
	}
}

func TestAddZonedNodeDC_ClampsNonPositiveWeight(t *testing.T) {
	// A non-positive weight is treated as 1.0, so the node still receives the
	// full default vnode count rather than zero.
	r := NewRing(150)

	r.AddZonedNodeDC("node1", "10.0.0.1", "us-east", "rack1", 0)

	stats := r.GetStats()
	if len(stats.Distribution) != 1 || stats.Distribution[0].VNodeCount != 150 {
		t.Fatalf("expected 150 vnodes from clamped weight, got %+v", stats.Distribution)
	}
}

func TestAddZonedNodeDC_ReAddReplacesVnodes(t *testing.T) {
	// Re-adding an existing node id (a metadata refresh from gossip) must clear
	// the old vnodes first; a lowered weight would otherwise leave orphaned
	// vnodes in the tree beyond the new count.
	r := NewRing(150)
	r.AddZonedNodeDC("node1", "10.0.0.1", "us-east", "rack1", 2.0) // 300 vnodes

	r.AddZonedNodeDC("node1", "10.0.0.1", "us-west", "rack2", 1.0) // 150 vnodes

	stats := r.GetStats()
	if stats.TotalVNodes != 150 {
		t.Fatalf("expected re-add to replace vnodes (150 total), got %d", stats.TotalVNodes)
	}
	if len(stats.Distribution) != 1 || stats.Distribution[0].DC != "us-west" {
		t.Fatalf("expected single node relabeled to us-west, got %+v", stats.Distribution)
	}
}

func TestAddZonedNode_LeavesDCEmpty(t *testing.T) {
	// The zone-only helper must delegate with an empty DC, so nodes added the
	// legacy way stay DC-unlabeled rather than inheriting a stray value.
	r := NewRing(150)

	r.AddZonedNode("node1", "10.0.0.1", "rack1", 1.0)

	nodes := r.GetNodes()
	if len(nodes) != 1 || nodes[0].DC != "" || nodes[0].Zone != "rack1" {
		t.Fatalf("expected node with empty dc and zone rack1, got %+v", nodes)
	}
}

func TestZonePlacement_SpreadsReplicasAcrossZones(t *testing.T) {
	// Arrange: six nodes across three zones, two per zone.
	r := newZonedRing(t, "a", "b", "c")

	// Act + Assert: every key's replica set covers all three zones.
	for i := 0; i < 200; i++ {
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 3)
		if len(nodes) != 3 {
			t.Fatalf("expected 3 replicas, got %d", len(nodes))
		}
		counts := zonesOf(nodes)
		if len(counts) != 3 {
			t.Errorf("key%d: expected replicas in 3 distinct zones, got %v", i, counts)
		}
	}
}

func TestZonePlacement_FallbackWhenFewerZonesThanFactor(t *testing.T) {
	// Arrange: four nodes across only two zones.
	r := newZonedRing(t, "a", "b")

	// Act + Assert: the set still fills to the factor, and the first two
	// replicas land in distinct zones before any zone repeats.
	for i := 0; i < 200; i++ {
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 3)
		if len(nodes) != 3 {
			t.Fatalf("key%d: expected 3 replicas despite only 2 zones, got %d", i, len(nodes))
		}
		if nodes[0].Zone == nodes[1].Zone {
			t.Errorf("key%d: first two replicas share zone %q", i, nodes[0].Zone)
		}
	}
}

func TestZonePlacement_SingleZoneStillFillsFactor(t *testing.T) {
	// Arrange: three nodes, all in the same zone.
	r := NewRing(150)
	r.AddZonedNode("node1", "10.0.0.1", "a", 1.0)
	r.AddZonedNode("node2", "10.0.0.2", "a", 1.0)
	r.AddZonedNode("node3", "10.0.0.3", "a", 1.0)

	// Act
	nodes := r.GetReplicationNodes("some-key", 3)

	// Assert
	if len(nodes) != 3 {
		t.Errorf("expected 3 replicas in a single-zone cluster, got %d", len(nodes))
	}
}

func TestZonePlacement_UnzonedNodesNeverConflict(t *testing.T) {
	// Arrange: one zoned pair plus two unzoned nodes.
	r := NewRing(150)
	r.AddZonedNode("a-1", "10.0.0.1", "a", 1.0)
	r.AddZonedNode("a-2", "10.0.0.2", "a", 1.0)
	r.AddNode("legacy1", "10.0.1.1")
	r.AddNode("legacy2", "10.0.1.2")

	// Act + Assert: unzoned nodes fill slots freely; only the second node
	// from zone "a" is deferred, so the factor always fills.
	for i := 0; i < 200; i++ {
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 3)
		if len(nodes) != 3 {
			t.Fatalf("key%d: expected 3 replicas, got %d", i, len(nodes))
		}
		if counts := zonesOf(nodes); counts["a"] > 1 {
			t.Errorf("key%d: zone a used %d slots while unzoned nodes remained", i, counts["a"])
		}
	}
}

func TestZonePlacement_PrimaryUnchangedByZones(t *testing.T) {
	// Arrange: zone awareness defers only conflicting followers; the first
	// clockwise owner must stay the primary, or anti-entropy's primary-range
	// bookkeeping and key lookup would disagree.
	r := newZonedRing(t, "a", "b", "c")

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%d", i)

		// Act
		primary, ok := r.GetNode(key)
		replicas := r.GetReplicationNodes(key, 3)

		// Assert
		if !ok || len(replicas) == 0 {
			t.Fatalf("key %s: lookup failed", key)
		}
		if replicas[0].ID != primary.ID {
			t.Errorf("key %s: replica[0] = %s but primary = %s", key, replicas[0].ID, primary.ID)
		}
	}
}

func TestZonePlacement_Deterministic(t *testing.T) {
	// Arrange
	r := newZonedRing(t, "a", "b", "c")

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)

		// Act
		first := r.GetReplicationNodes(key, 3)
		second := r.GetReplicationNodes(key, 3)

		// Assert
		for j := range first {
			if first[j].ID != second[j].ID {
				t.Fatalf("key %s: replica sets differ between calls", key)
			}
		}
	}
}

func TestGetHealthyReplicationNodes_ZoneAwareSkipReportsOwner(t *testing.T) {
	// Arrange: kill one intended owner; its zone twin should substitute and
	// keep the healthy set spread across all three zones.
	r := newZonedRing(t, "a", "b", "c")
	owners := r.GetReplicationNodes("some-key", 3)
	down := owners[1].ID
	r.SetHealthFilter(func(nodeID string) bool { return nodeID != down })

	// Act
	nodes, skipped := r.GetHealthyReplicationNodes("some-key", 3)

	// Assert
	if len(skipped) != 1 || skipped[0].ID != down {
		t.Fatalf("expected skipped = [%s], got %v", down, skipped)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 healthy replicas, got %d", len(nodes))
	}
	if counts := zonesOf(nodes); len(counts) != 3 {
		t.Errorf("expected substitute to restore 3-zone coverage, got %v", counts)
	}
	for _, n := range nodes {
		if n.ID == down {
			t.Errorf("unhealthy node %s appeared in healthy set", down)
		}
	}
}

func TestGetHealthyReplicationNodes_WholeZoneDown(t *testing.T) {
	// Arrange: take down every node in one intended owner's zone.
	r := newZonedRing(t, "a", "b", "c")
	owners := r.GetReplicationNodes("some-key", 3)
	deadZone := owners[2].Zone
	r.SetHealthFilter(func(nodeID string) bool {
		for _, n := range r.GetNodes() {
			if n.ID == nodeID {
				return n.Zone != deadZone
			}
		}
		return false
	})

	// Act
	nodes, skipped := r.GetHealthyReplicationNodes("some-key", 3)

	// Assert: the set still fills from the surviving zones, and the dead
	// zone's owner is reported for hinting.
	if len(nodes) != 3 {
		t.Fatalf("expected 3 healthy replicas, got %d", len(nodes))
	}
	for _, n := range nodes {
		if n.Zone == deadZone {
			t.Errorf("node %s from dead zone %q in healthy set", n.ID, deadZone)
		}
	}
	if len(skipped) != 1 || skipped[0].Zone != deadZone {
		t.Errorf("expected one skipped owner in zone %q, got %v", deadZone, skipped)
	}
}

func TestGetHealthyReplicationNodes_SkippedAreAlwaysIntendedOwners(t *testing.T) {
	// Arrange: hints must target real owners, never zone-deferred bystanders.
	r := newZonedRing(t, "a", "b", "c")

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		owners := r.GetReplicationNodes(key, 3)
		down := owners[0].ID
		r.SetHealthFilter(func(nodeID string) bool { return nodeID != down })

		// Act
		_, skipped := r.GetHealthyReplicationNodes(key, 3)

		// Assert
		for _, s := range skipped {
			if !nodeInSet(owners, s.ID) {
				t.Errorf("key %s: skipped node %s is not an intended owner", key, s.ID)
			}
		}
	}
}

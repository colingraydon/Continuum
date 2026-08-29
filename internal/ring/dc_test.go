package ring

import (
	"fmt"
	"testing"
)

// newDCRing builds a ring from a dc -> zones topology, one node per zone entry.
// Repeat a zone name to put two nodes in it. Node IDs are "<dc>/<zone>/<i>".
func newDCRing(t *testing.T, topo map[string][]string) *Ring {
	t.Helper()
	r := NewRing(150)
	host := 0
	for dc, zones := range topo {
		for i, z := range zones {
			host++
			r.AddZonedNodeDC(
				fmt.Sprintf("%s/%s/%d", dc, z, i),
				fmt.Sprintf("10.0.0.%d", host),
				dc, z, 1.0,
			)
		}
	}
	return r
}

// twoDCRing is the standard fixture: two DCs, three zones each, two nodes per
// zone — enough nodes that a target of 3 per DC can be met from distinct zones
// and still have healthy substitutes in reserve.
func twoDCRing(t *testing.T) *Ring {
	t.Helper()
	return newDCRing(t, map[string][]string{
		"us-east": {"rack1", "rack1", "rack2", "rack2", "rack3", "rack3"},
		"eu-west": {"rack1", "rack1", "rack2", "rack2", "rack3", "rack3"},
	})
}

func dcsOf(nodes []*Node) map[string]int {
	counts := make(map[string]int)
	for _, n := range nodes {
		counts[n.DC]++
	}
	return counts
}

func TestSetDCReplication_TotalAndClear(t *testing.T) {
	// Arrange
	r := NewRing(150)

	// Act
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 2})

	// Assert
	if got := r.DCReplicationTotal(); got != 5 {
		t.Errorf("total = %d, want 5", got)
	}

	// Act: an empty table restores single-DC placement.
	r.SetDCReplication(nil)

	// Assert
	if got := r.DCReplicationTotal(); got != 0 {
		t.Errorf("total after clear = %d, want 0", got)
	}
}

func TestSetDCReplication_DropsNonPositiveTargets(t *testing.T) {
	// A zero or negative target means "no replicas here", which is the same as
	// omitting the DC — keeping it would let a DC occupy the table with no
	// slots to fill.
	r := NewRing(150)

	r.SetDCReplication(map[string]int{"us-east": 3, "dead-dc": 0, "bad-dc": -1})

	if got := r.DCReplicationTotal(); got != 3 {
		t.Errorf("total = %d, want 3 (non-positive targets dropped)", got)
	}
}

func TestSetDCReplication_CopiesCallerMap(t *testing.T) {
	// The ring must not alias the caller's map: a later mutation would silently
	// change placement without any lock held.
	r := NewRing(150)
	factors := map[string]int{"us-east": 3}

	r.SetDCReplication(factors)
	factors["eu-west"] = 99

	if got := r.DCReplicationTotal(); got != 3 {
		t.Errorf("total = %d, want 3 — ring aliased the caller's map", got)
	}
}

func TestDCPlacement_HonorsPerDCTargets(t *testing.T) {
	// Arrange
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 2})

	// Act + Assert: every key keeps exactly the configured count in each DC.
	for i := 0; i < 200; i++ {
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 5)
		counts := dcsOf(nodes)
		if counts["us-east"] != 3 || counts["eu-west"] != 2 {
			t.Fatalf("key%d: per-DC counts = %v, want us-east:3 eu-west:2", i, counts)
		}
	}
}

func TestDCPlacement_SpreadsZonesWithinEachDC(t *testing.T) {
	// Arrange: three zones per DC, target 3 per DC — every DC's replica set
	// should cover all three of its own zones.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 200; i++ {
		// Act
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 6)

		// Assert
		perDC := make(map[string]map[string]bool)
		for _, n := range nodes {
			if perDC[n.DC] == nil {
				perDC[n.DC] = make(map[string]bool)
			}
			perDC[n.DC][n.Zone] = true
		}
		for dc, zones := range perDC {
			if len(zones) != 3 {
				t.Fatalf("key%d: dc %s covered %d zones, want 3", i, dc, len(zones))
			}
		}
	}
}

func TestDCPlacement_SameZoneNameInDifferentDCsDoNotConflict(t *testing.T) {
	// Arrange: both DCs name their racks identically. Zone uniqueness is scoped
	// per DC, so us-east/rack1 and eu-west/rack1 are distinct failure domains
	// and must both be selectable.
	r := newDCRing(t, map[string][]string{
		"us-east": {"rack1", "rack2"},
		"eu-west": {"rack1", "rack2"},
	})
	r.SetDCReplication(map[string]int{"us-east": 2, "eu-west": 2})

	for i := 0; i < 100; i++ {
		// Act
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 4)

		// Assert: all four nodes selected, two per DC.
		if len(nodes) != 4 {
			t.Fatalf("key%d: got %d replicas, want 4", i, len(nodes))
		}
		if counts := dcsOf(nodes); counts["us-east"] != 2 || counts["eu-west"] != 2 {
			t.Fatalf("key%d: per-DC counts = %v, want 2 each", i, counts)
		}
	}
}

func TestDCPlacement_UnlistedDCGetsNoReplicas(t *testing.T) {
	// Arrange: a third DC exists in the ring but is absent from the table.
	r := newDCRing(t, map[string][]string{
		"us-east":  {"rack1", "rack2", "rack3"},
		"eu-west":  {"rack1", "rack2", "rack3"},
		"ap-south": {"rack1", "rack2", "rack3"},
	})
	r.SetDCReplication(map[string]int{"us-east": 2, "eu-west": 2})

	for i := 0; i < 200; i++ {
		// Act
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 4)

		// Assert
		if counts := dcsOf(nodes); counts["ap-south"] != 0 {
			t.Fatalf("key%d: unlisted DC took %d slots, want 0", i, counts["ap-south"])
		}
	}
}

func TestDCPlacement_ShortDCDoesNotBorrowFromAnother(t *testing.T) {
	// Arrange: us-east has only two nodes but asks for three. The set comes back
	// short rather than topping up from eu-west — moving that replica across the
	// WAN would defeat the per-DC durability the table asks for.
	r := newDCRing(t, map[string][]string{
		"us-east": {"rack1", "rack2"},
		"eu-west": {"rack1", "rack2", "rack3"},
	})
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 100; i++ {
		// Act
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 6)

		// Assert
		counts := dcsOf(nodes)
		if counts["us-east"] != 2 {
			t.Fatalf("key%d: us-east = %d, want 2 (all it has)", i, counts["us-east"])
		}
		if counts["eu-west"] != 3 {
			t.Fatalf("key%d: eu-west = %d, want 3", i, counts["eu-west"])
		}
	}
}

func TestDCPlacement_FewerZonesThanTargetStillFills(t *testing.T) {
	// Arrange: us-east has three nodes but only two zones. Zone spreading is a
	// preference, not a cap: the DC still reaches its target of 3 by falling
	// back to a repeated zone, mirroring the single-DC walk.
	r := newDCRing(t, map[string][]string{
		"us-east": {"rack1", "rack1", "rack2"},
		"eu-west": {"rack1", "rack2", "rack3"},
	})
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 100; i++ {
		// Act
		nodes := r.GetReplicationNodes(fmt.Sprintf("key%d", i), 6)

		// Assert
		if counts := dcsOf(nodes); counts["us-east"] != 3 {
			t.Fatalf("key%d: us-east = %d, want 3 despite only 2 zones", i, counts["us-east"])
		}
	}
}

func TestDCPlacement_Deterministic(t *testing.T) {
	// Arrange
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)

		// Act
		first := r.GetReplicationNodes(key, 6)
		second := r.GetReplicationNodes(key, 6)

		// Assert
		if len(first) != len(second) {
			t.Fatalf("key %s: set size differs between calls", key)
		}
		for j := range first {
			if first[j].ID != second[j].ID {
				t.Fatalf("key %s: replica sets differ between calls", key)
			}
		}
	}
}

func TestDCPlacement_TableOverridesRequestedFactor(t *testing.T) {
	// Arrange: with a per-DC table installed the table is authoritative — the
	// caller's factor (which reaches the ring from POST /replicate bodies) no
	// longer sizes the set.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 2, "eu-west": 2})

	// Act
	nodes := r.GetReplicationNodes("some-key", 12)

	// Assert
	if len(nodes) != 4 {
		t.Errorf("got %d replicas, want 4 from the table regardless of factor", len(nodes))
	}
}

func TestDCPlacement_NoTableLeavesPlacementUnchanged(t *testing.T) {
	// Arrange: without a table the DC labels carry no placement weight, so the
	// set is sized by the caller's factor exactly as before.
	r := twoDCRing(t)

	// Act
	nodes := r.GetReplicationNodes("some-key", 3)

	// Assert
	if len(nodes) != 3 {
		t.Errorf("got %d replicas, want 3 from the requested factor", len(nodes))
	}
}

func TestGetHealthyReplicationNodesDC_SubstituteStaysInSameDC(t *testing.T) {
	// Arrange: kill one us-east owner. Its slot must be refilled from us-east,
	// not from eu-west.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})
	owners := r.GetReplicationNodes("some-key", 6)

	var down *Node
	for _, n := range owners {
		if n.DC == "us-east" {
			down = n
			break
		}
	}
	if down == nil {
		t.Fatal("fixture produced no us-east owner")
	}
	r.SetHealthFilter(func(nodeID string) bool { return nodeID != down.ID })

	// Act
	nodes, skipped := r.GetHealthyReplicationNodes("some-key", 6)

	// Assert
	if len(skipped) != 1 || skipped[0].ID != down.ID {
		t.Fatalf("skipped = %v, want [%s]", skipped, down.ID)
	}
	if counts := dcsOf(nodes); counts["us-east"] != 3 || counts["eu-west"] != 3 {
		t.Errorf("per-DC counts = %v, want 3 each — substitute crossed a DC boundary", counts)
	}
	for _, n := range nodes {
		if n.ID == down.ID {
			t.Errorf("unhealthy node %s appeared in healthy set", down.ID)
		}
	}
}

func TestGetHealthyReplicationNodesDC_WholeDCDownLeavesRemoteSetIntact(t *testing.T) {
	// Arrange: the whole of us-east is unreachable — a DC outage or a severed
	// WAN link. eu-west keeps its full replica set, and every us-east owner is
	// reported for hinting rather than being replaced from across the WAN.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})
	r.SetHealthFilter(func(nodeID string) bool {
		for _, n := range r.GetNodes() {
			if n.ID == nodeID {
				return n.DC != "us-east"
			}
		}
		return false
	})

	// Act
	nodes, skipped := r.GetHealthyReplicationNodes("some-key", 6)

	// Assert
	counts := dcsOf(nodes)
	if counts["us-east"] != 0 {
		t.Errorf("us-east contributed %d healthy replicas, want 0", counts["us-east"])
	}
	if counts["eu-west"] != 3 {
		t.Errorf("eu-west = %d healthy replicas, want 3", counts["eu-west"])
	}
	if len(skipped) != 3 {
		t.Errorf("skipped = %d owners, want 3 (the whole us-east replica set)", len(skipped))
	}
	for _, n := range skipped {
		if n.DC != "us-east" {
			t.Errorf("skipped node %s is in %s, want us-east", n.ID, n.DC)
		}
	}
}

func TestGetHealthyReplicationNodesDC_SkippedAreAlwaysIntendedOwners(t *testing.T) {
	// Hints must target real owners, never zone- or DC-deferred bystanders.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		owners := r.GetReplicationNodes(key, 6)
		down := owners[0].ID
		r.SetHealthFilter(func(nodeID string) bool { return nodeID != down })

		// Act
		_, skipped := r.GetHealthyReplicationNodes(key, 6)

		// Assert
		for _, s := range skipped {
			if !nodeInSet(owners, s.ID) {
				t.Errorf("key %s: skipped node %s is not an intended owner", key, s.ID)
			}
		}
	}
}

func TestGetHealthyReplicationNodesDC_AllHealthyMatchesStrict(t *testing.T) {
	// With every node healthy the sloppy variant must agree exactly with the
	// strict walk and report nothing skipped.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})
	r.SetHealthFilter(func(string) bool { return true })

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)

		// Act
		strict := r.GetReplicationNodes(key, 6)
		healthy, skipped := r.GetHealthyReplicationNodes(key, 6)

		// Assert
		if len(skipped) != 0 {
			t.Fatalf("key %s: skipped = %v, want empty", key, skipped)
		}
		if len(healthy) != len(strict) {
			t.Fatalf("key %s: healthy set size %d != strict %d", key, len(healthy), len(strict))
		}
		for j := range strict {
			if healthy[j].ID != strict[j].ID {
				t.Fatalf("key %s: healthy set differs from strict walk", key)
			}
		}
	}
}

func TestDCPlacement_EmptyRingReturnsNothing(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.SetDCReplication(map[string]int{"us-east": 3})

	// Act
	nodes := r.GetReplicationNodes("some-key", 3)
	healthy, skipped := r.GetHealthyReplicationNodes("some-key", 3)

	// Assert
	if len(nodes) != 0 || len(healthy) != 0 || len(skipped) != 0 {
		t.Errorf("empty ring returned nodes=%v healthy=%v skipped=%v", nodes, healthy, skipped)
	}
}

func TestZoneScoping_SameZoneNameAcrossDCsWithoutTable(t *testing.T) {
	// Even with no per-DC table, zone uniqueness is scoped by DC: rack1 in
	// us-east and rack1 in eu-west are different failure domains, so both can
	// hold a replica of the same key.
	r := newDCRing(t, map[string][]string{
		"us-east": {"rack1"},
		"eu-west": {"rack1"},
	})

	nodes := r.GetReplicationNodes("some-key", 2)

	if len(nodes) != 2 {
		t.Fatalf("got %d replicas, want 2 — zones collided across DCs", len(nodes))
	}
}

func TestDCPlacement_PrimaryPreservedWhenAllDCsListed(t *testing.T) {
	// When the table names every DC in the ring, the first clockwise owner is
	// still the primary — the DC walk defers conflicting followers, exactly as
	// the single-DC walk defers zone-conflicting ones.
	r := twoDCRing(t)
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 3})

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%d", i)

		// Act
		primary, ok := r.GetNode(key)
		replicas := r.GetReplicationNodes(key, 6)

		// Assert
		if !ok || len(replicas) == 0 {
			t.Fatalf("key %s: lookup failed", key)
		}
		if replicas[0].ID != primary.ID {
			t.Errorf("key %s: replica[0] = %s but primary = %s", key, replicas[0].ID, primary.ID)
		}
	}
}

func TestDCPlacement_PrimaryMayNotOwnKeyWhenDCUnlisted(t *testing.T) {
	// Documents the one case where replica[0] and the primary diverge: the
	// first clockwise node sits in a DC the table omits, so it holds no replica
	// of the key. Nothing load-bearing depends on the invariant — GetNode backs
	// only the informational /node endpoint, the read/write paths use
	// GetHealthyReplicationNodes, and anti-entropy's primary ranges are purely
	// token-based — but the divergence is real and worth pinning.
	r := newDCRing(t, map[string][]string{
		"us-east":  {"rack1", "rack2", "rack3"},
		"ap-south": {"rack1", "rack2", "rack3"},
	})
	r.SetDCReplication(map[string]int{"us-east": 2})

	diverged := false
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%d", i)
		primary, _ := r.GetNode(key)
		replicas := r.GetReplicationNodes(key, 2)

		// Every returned replica is in the listed DC, whatever the primary is.
		for _, n := range replicas {
			if n.DC != "us-east" {
				t.Fatalf("key %s: replica %s is in unlisted DC %s", key, n.ID, n.DC)
			}
		}
		if len(replicas) > 0 && replicas[0].ID != primary.ID {
			diverged = true
		}
	}
	if !diverged {
		t.Error("expected at least one key whose primary is not its first replica")
	}
}

func TestDCReplicationFactor(t *testing.T) {
	// Arrange
	r := NewRing(150)

	// Assert: with no table installed every DC reports zero, which is what
	// disqualifies a LOCAL_ consistency level.
	if got := r.DCReplicationFactor("us-east"); got != 0 {
		t.Errorf("without a table: got %d, want 0", got)
	}

	// Act
	r.SetDCReplication(map[string]int{"us-east": 3, "eu-west": 2})

	// Assert
	if got := r.DCReplicationFactor("us-east"); got != 3 {
		t.Errorf("us-east = %d, want 3", got)
	}
	if got := r.DCReplicationFactor("eu-west"); got != 2 {
		t.Errorf("eu-west = %d, want 2", got)
	}
	if got := r.DCReplicationFactor("ap-south"); got != 0 {
		t.Errorf("unlisted DC = %d, want 0", got)
	}
	if got := r.DCReplicationFactor(""); got != 0 {
		t.Errorf("empty DC = %d, want 0", got)
	}
}

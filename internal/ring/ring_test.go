package ring

import "testing"

func TestNewRing(t *testing.T) {
	// Arrange + Act
	ring := NewRing(150)

	// Assert
	if ring == nil {
		t.Fatal("expected ring to not be nil")
	}
	if ring.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", ring.NodeCount())
	}
}

func TestAddNode(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act
	ring.AddNode("node1", "10.0.0.1")

	// Assert
	if ring.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", ring.NodeCount())
	}
}

func TestAddMultipleNodes(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")
	ring.AddNode("node3", "10.0.0.3")

	// Assert
	if ring.NodeCount() != 3 {
		t.Errorf("expected 3 nodes, got %d", ring.NodeCount())
	}
}

func TestRemoveNode(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")

	// Act
	ring.RemoveNode("node1")

	// Assert
	if ring.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", ring.NodeCount())
	}
}

func TestRemoveNonExistentNode(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act + Assert
	ring.RemoveNode("nonexistent")
}

func TestRemoveOnlyRemovesTargetNode(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")

	// Act
	ring.RemoveNode("node1")

	// Assert
	if ring.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", ring.NodeCount())
	}
}

func TestGetNode(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")

	// Act
	node, found := ring.GetNode("somekey")

	// Assert
	if !found {
		t.Fatal("expected to find a node")
	}
	if node.ID != "node1" {
		t.Errorf("expected node1, got %s", node.ID)
	}
}

func TestGetNodeEmptyRing(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act
	node, found := ring.GetNode("somekey")

	// Assert
	if found {
		t.Fatal("expected no node from empty ring")
	}
	if node != nil {
		t.Fatal("expected nil node from empty ring")
	}
}

func TestGetNodeIsDeterministic(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")

	// Act
	node1, _ := ring.GetNode("somekey")
	node2, _ := ring.GetNode("somekey")

	// Assert
	if node1.ID != node2.ID {
		t.Errorf("expected deterministic lookup, got %s and %s", node1.ID, node2.ID)
	}
}

func TestGetNodeMinimalKeyMovementOnAddition(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")

	keys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}
	before := make(map[string]string)
	for _, key := range keys {
		node, _ := ring.GetNode(key)
		before[key] = node.ID
	}

	// Act
	ring.AddNode("node3", "10.0.0.3")

	// Assert
	moved := 0
	for _, key := range keys {
		node, _ := ring.GetNode(key)
		if node.ID != before[key] {
			if node.ID != "node3" {
				t.Errorf("key %s moved from %s to %s, expected it to only move to node3", key, before[key], node.ID)
			}
			moved++
		}
	}

	if moved == len(keys) {
		t.Error("all keys moved, expected only a subset to move")
	}
}

func TestGetReplicationNodes(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")
	ring.AddNode("node3", "10.0.0.3")

	// Act
	nodes := ring.GetReplicationNodes("somekey", 3)

	// Assert
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
}

func TestGetReplicationNodesAreDistinct(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")
	ring.AddNode("node3", "10.0.0.3")

	// Act
	nodes := ring.GetReplicationNodes("somekey", 3)

	// Assert
	seen := make(map[string]bool)
	for _, node := range nodes {
		if seen[node.ID] {
			t.Errorf("duplicate node %s in replication set", node.ID)
		}
		seen[node.ID] = true
	}
}

func TestGetReplicationNodesEmptyRing(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act
	nodes := ring.GetReplicationNodes("somekey", 3)

	// Assert
	if nodes != nil {
		t.Fatal("expected nil from empty ring")
	}
}

func TestGetReplicationNodesFewerNodesThanFactor(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")

	// Act
	nodes := ring.GetReplicationNodes("somekey", 3)

	// Assert
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestGetNodes(t *testing.T) {
	// Arrange
	ring := NewRing(150)
	ring.AddNode("node1", "10.0.0.1")
	ring.AddNode("node2", "10.0.0.2")
	ring.AddNode("node3", "10.0.0.3")

	// Act
	nodes := ring.GetNodes()

	// Assert
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
}

func TestGetNodesEmpty(t *testing.T) {
	// Arrange
	ring := NewRing(150)

	// Act
	nodes := ring.GetNodes()

	// Assert
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestSetUpdateCallback(t *testing.T) {
	// Arrange
	r := NewRing(150)
	var capturedNodeCount, capturedVNodeCount int
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		capturedNodeCount = nodeCount
		capturedVNodeCount = vnodeCount
	})

	// Act
	r.AddNode("node1", "10.0.0.1")

	// Assert
	if capturedNodeCount != 1 {
		t.Errorf("expected node count 1, got %d", capturedNodeCount)
	}
	if capturedVNodeCount != 150 {
		t.Errorf("expected vnode count 150, got %d", capturedVNodeCount)
	}
}

func TestSetUpdateCallbackOnRemove(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	var capturedNodeCount, capturedVNodeCount int
	r.SetUpdateCallback(func(nodeCount, vnodeCount int) {
		capturedNodeCount = nodeCount
		capturedVNodeCount = vnodeCount
	})

	// Act
	r.RemoveNode("node1")

	// Assert
	if capturedNodeCount != 0 {
		t.Errorf("expected node count 0, got %d", capturedNodeCount)
	}
	if capturedVNodeCount != 0 {
		t.Errorf("expected vnode count 0, got %d", capturedVNodeCount)
	}
}

func TestHealthFilterSkipsSuspectNode(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	healthy := map[string]bool{"node1": true, "node2": true}
	r.SetHealthFilter(func(id string) bool { return healthy[id] })

	// Act - mark node that owns the key as unhealthy
	first, _ := r.GetNode("mykey")
	healthy[first.ID] = false
	second, found := r.GetNode("mykey")

	// Assert
	if !found {
		t.Fatal("expected a node to be found after filtering")
	}
	if second.ID == first.ID {
		t.Errorf("expected a different node after filtering, got %s again", second.ID)
	}
}

func TestHealthFilterNoHealthyNodes(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.SetHealthFilter(func(id string) bool { return false })

	// Act
	_, found := r.GetNode("mykey")

	// Assert
	if found {
		t.Error("expected no node when all nodes are unhealthy")
	}
}

// --- VnodeRange.Contains tests ---

func TestVnodeRangeContainsNonWrapping(t *testing.T) {
	vr := VnodeRange{Start: 10, End: 20}
	cases := []struct {
		hash uint32
		want bool
	}{
		{15, true},  // inside
		{10, false}, // start is exclusive
		{20, true},  // end is inclusive
		{5, false},  // below range
		{25, false}, // above range
	}
	for _, tc := range cases {
		if got := vr.Contains(tc.hash); got != tc.want {
			t.Errorf("Contains(%d): expected %v, got %v", tc.hash, tc.want, got)
		}
	}
}

func TestVnodeRangeContainsWrapping(t *testing.T) {
	// Start > End means the range wraps around zero.
	vr := VnodeRange{Start: 200, End: 50}
	cases := []struct {
		hash uint32
		want bool
	}{
		{210, true},  // above Start
		{30, true},   // below End
		{50, true},   // End is inclusive
		{200, false}, // Start is exclusive
		{100, false}, // in the gap between End and Start
	}
	for _, tc := range cases {
		if got := vr.Contains(tc.hash); got != tc.want {
			t.Errorf("Contains(%d): expected %v, got %v", tc.hash, tc.want, got)
		}
	}
}

// --- GetPrimaryVnodeRanges tests ---

func TestGetPrimaryVnodeRangesEmptyRing(t *testing.T) {
	r := NewRing(10)
	if ranges := r.GetPrimaryVnodeRanges("node1"); ranges != nil {
		t.Fatalf("expected nil for empty ring, got %v", ranges)
	}
}

func TestGetPrimaryVnodeRangesSingleNode(t *testing.T) {
	const replicas = 5
	r := NewRing(replicas)
	r.AddNode("node1", "10.0.0.1")

	ranges := r.GetPrimaryVnodeRanges("node1")
	if len(ranges) != replicas {
		t.Fatalf("expected %d ranges for single node, got %d", replicas, len(ranges))
	}
}

func TestGetPrimaryVnodeRangesUnknownNode(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1", "10.0.0.1")

	ranges := r.GetPrimaryVnodeRanges("ghost")
	if len(ranges) != 0 {
		t.Fatalf("expected 0 ranges for unknown node, got %d", len(ranges))
	}
}

func TestGetPrimaryVnodeRangesTotalCount(t *testing.T) {
	const replicas = 5
	r := NewRing(replicas)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	total := len(r.GetPrimaryVnodeRanges("node1")) +
		len(r.GetPrimaryVnodeRanges("node2")) +
		len(r.GetPrimaryVnodeRanges("node3"))

	want := replicas * 3
	if total != want {
		t.Errorf("expected %d total primary ranges across all nodes, got %d", want, total)
	}
}

func TestGetPrimaryVnodeRangesPartitionCoverage(t *testing.T) {
	// Every key hash must fall into exactly one node's primary ranges.
	const replicas = 10
	r := NewRing(replicas)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	allRanges := append(
		r.GetPrimaryVnodeRanges("node1"),
		append(r.GetPrimaryVnodeRanges("node2"), r.GetPrimaryVnodeRanges("node3")...)...,
	)

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	for _, key := range keys {
		hash := computeHash(key)
		matches := 0
		for _, vr := range allRanges {
			if vr.Contains(hash) {
				matches++
			}
		}
		if matches != 1 {
			t.Errorf("key %q (hash %d) matched %d ranges, expected exactly 1", key, hash, matches)
		}
	}
}

// --- AddWeightedNode tests ---

func TestAddWeightedNode_DoubleWeight(t *testing.T) {
	const base = 10
	r := NewRing(base)
	r.AddWeightedNode("heavy", "10.0.0.1", 2.0)
	r.AddWeightedNode("normal", "10.0.0.2", 1.0)

	if r.vnodeCounts["heavy"] != 20 {
		t.Errorf("expected 20 vnodes for weight 2.0, got %d", r.vnodeCounts["heavy"])
	}
	if r.vnodeCounts["normal"] != 10 {
		t.Errorf("expected 10 vnodes for weight 1.0, got %d", r.vnodeCounts["normal"])
	}
}

func TestAddWeightedNode_HalfWeight(t *testing.T) {
	const base = 10
	r := NewRing(base)
	r.AddWeightedNode("light", "10.0.0.1", 0.5)

	if r.vnodeCounts["light"] != 5 {
		t.Errorf("expected 5 vnodes for weight 0.5, got %d", r.vnodeCounts["light"])
	}
}

func TestAddWeightedNode_ZeroWeightClampsToDefault(t *testing.T) {
	r := NewRing(10)
	r.AddWeightedNode("n", "10.0.0.1", 0)

	if r.vnodeCounts["n"] != 10 {
		t.Errorf("expected 10 vnodes for weight 0 (default), got %d", r.vnodeCounts["n"])
	}
}

func TestAddWeightedNode_NegativeWeightClampsToDefault(t *testing.T) {
	r := NewRing(10)
	r.AddWeightedNode("n", "10.0.0.1", -1.0)

	if r.vnodeCounts["n"] != 10 {
		t.Errorf("expected 10 vnodes for negative weight, got %d", r.vnodeCounts["n"])
	}
}

func TestAddWeightedNode_RemoveUsesCorrectCount(t *testing.T) {
	r := NewRing(10)
	r.AddWeightedNode("heavy", "10.0.0.1", 2.0)
	r.AddWeightedNode("normal", "10.0.0.2", 1.0)

	r.RemoveNode("heavy")

	if r.NodeCount() != 1 {
		t.Fatalf("expected 1 node after remove, got %d", r.NodeCount())
	}
	// All remaining vnodes should belong to normal (10 vnodes).
	if r.tree.Tree.Size() != 10 {
		t.Errorf("expected 10 vnodes after removing heavy node, got %d", r.tree.Tree.Size())
	}
}

func TestAddWeightedNode_HeavierNodeGetsMoreKeys(t *testing.T) {
	// A node with 3x weight should attract roughly 3x the keys.
	r := NewRing(100)
	r.AddWeightedNode("heavy", "10.0.0.1", 3.0)
	r.AddWeightedNode("normal", "10.0.0.2", 1.0)

	heavyCount, normalCount := 0, 0
	for i := 0; i < 10000; i++ {
		key := generateHashInput("key", i)
		node, _ := r.GetNode(key)
		if node.ID == "heavy" {
			heavyCount++
		} else {
			normalCount++
		}
	}

	ratio := float64(heavyCount) / float64(normalCount)
	// Expect roughly 3:1; allow 20% tolerance.
	if ratio < 2.0 || ratio > 4.0 {
		t.Errorf("expected heavy:normal ratio near 3.0, got %.2f (%d:%d)", ratio, heavyCount, normalCount)
	}
}

func TestAddNode_DefaultWeightEqualsOne(t *testing.T) {
	r := NewRing(50)
	r.AddNode("n", "10.0.0.1")

	if r.vnodeCounts["n"] != 50 {
		t.Errorf("AddNode should use weight 1.0 (50 vnodes), got %d", r.vnodeCounts["n"])
	}
}

func TestAddWeightedNode_VnodesClampedToOne(t *testing.T) {
	// replicas=1, weight=0.4 → round(0.4)=0 → clamped to 1.
	r := NewRing(1)
	r.AddWeightedNode("n", "10.0.0.1", 0.4)
	if r.vnodeCounts["n"] != 1 {
		t.Errorf("expected vnodes clamped to 1, got %d", r.vnodeCounts["n"])
	}
}

func TestGetReplicationNodes_WrapAround(t *testing.T) {
	// With 2 nodes and factor 2, and a key that sorts after all vnodes in the tree,
	// GetReplicationNodes must wrap around and still return 2 distinct nodes.
	r := NewRing(3)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	// Try many keys to exercise the wrap-around path.
	for _, key := range []string{"wrap1", "wrap2", "wrap3", "wrap4", "wrap5"} {
		nodes := r.GetReplicationNodes(key, 2)
		if len(nodes) != 2 {
			t.Errorf("key %s: expected 2 nodes, got %d", key, len(nodes))
		}
		seen := make(map[string]bool)
		for _, n := range nodes {
			if seen[n.ID] {
				t.Errorf("key %s: duplicate node %s", key, n.ID)
			}
			seen[n.ID] = true
		}
	}
}

func TestGetReplicationNodesForHash_WrapAround(t *testing.T) {
	r := NewRing(3)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	// Use hash=0 which is before all vnodes → forces wrap-around iteration.
	nodes := r.GetReplicationNodesForHash(0, 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes on wrap-around, got %d", len(nodes))
	}
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n.ID] {
			t.Errorf("duplicate node %s in wrap-around result", n.ID)
		}
		seen[n.ID] = true
	}
}

// --- GetReplicationNodesForHash tests ---

func TestGetReplicationNodesForHash_EmptyRing(t *testing.T) {
	r := NewRing(10)
	if nodes := r.GetReplicationNodesForHash(0, 3); nodes != nil {
		t.Fatalf("expected nil for empty ring, got %v", nodes)
	}
}

func TestGetReplicationNodesForHash_FewerNodesThanFactor(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	nodes := r.GetReplicationNodesForHash(0, 5)
	if len(nodes) != 2 {
		t.Errorf("expected 2 (capped at node count), got %d", len(nodes))
	}
}

func TestGetReplicationNodesForHash_ReturnsDistinctNodes(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	ranges := r.GetPrimaryVnodeRanges("node1")
	if len(ranges) == 0 {
		t.Fatal("expected primary vnode ranges")
	}
	nodes := r.GetReplicationNodesForHash(ranges[0].End, 3)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}
	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n.ID] {
			t.Errorf("duplicate node %s", n.ID)
		}
		seen[n.ID] = true
	}
}

func TestGetReplicationNodesForHash_ConsistentWithKeyLookup(t *testing.T) {
	r := NewRing(50)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	key := "testkey"
	keyNodes := r.GetReplicationNodes(key, 2)
	hashNodes := r.GetReplicationNodesForHash(computeHash(key), 2)

	if len(keyNodes) != len(hashNodes) {
		t.Fatalf("expected same length: key=%d hash=%d", len(keyNodes), len(hashNodes))
	}
	for i := range keyNodes {
		if keyNodes[i].ID != hashNodes[i].ID {
			t.Errorf("pos %d: key lookup=%s, hash lookup=%s", i, keyNodes[i].ID, hashNodes[i].ID)
		}
	}
}

// --- GetVnodeRange tests ---

func TestGetVnodeRange_EmptyRing(t *testing.T) {
	r := NewRing(10)
	_, ok := r.GetVnodeRange(0)
	if ok {
		t.Error("expected false for empty ring")
	}
}

func TestGetVnodeRange_ExistingEndHash(t *testing.T) {
	r := NewRing(5)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	for _, want := range r.GetPrimaryVnodeRanges("node1") {
		vr, ok := r.GetVnodeRange(want.End)
		if !ok {
			t.Errorf("GetVnodeRange(%d) returned false", want.End)
			continue
		}
		if vr.End != want.End {
			t.Errorf("expected End=%d, got %d", want.End, vr.End)
		}
		// The range's End should be contained in the range itself.
		if !vr.Contains(vr.End) {
			t.Errorf("range does not contain its own End hash %d", vr.End)
		}
	}
}

func TestGetVnodeRange_UnknownHash(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1", "10.0.0.1")

	known := make(map[uint32]bool)
	for _, vr := range r.GetPrimaryVnodeRanges("node1") {
		known[vr.End] = true
	}
	var missing uint32
	for h := uint32(1); ; h++ {
		if !known[h] {
			missing = h
			break
		}
	}

	_, ok := r.GetVnodeRange(missing)
	if ok {
		t.Errorf("GetVnodeRange(%d) should return false for non-vnode hash", missing)
	}
}

func TestGetVnodeRange_WrapAroundSingleNode(t *testing.T) {
	// With a single node, the first vnode's predecessor is the last vnode,
	// which produces a wrapping range (Start > End for the first vnode).
	r := NewRing(3)
	r.AddNode("node1", "10.0.0.1")

	ranges := r.GetPrimaryVnodeRanges("node1")
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
	// At least one range must wrap (Start >= End) since there is only one node.
	hasWrap := false
	for _, vr := range ranges {
		if vr.Start >= vr.End {
			hasWrap = true
		}
		// Verify the retrieved range matches.
		got, ok := r.GetVnodeRange(vr.End)
		if !ok {
			t.Errorf("GetVnodeRange(%d) returned false", vr.End)
			continue
		}
		if got.Start != vr.Start || got.End != vr.End {
			t.Errorf("range mismatch: got {%d,%d}, want {%d,%d}", got.Start, got.End, vr.Start, vr.End)
		}
	}
	if !hasWrap {
		t.Error("expected at least one wrapping range for single-node ring")
	}
}
func TestGetReplicationNodes_FactorOne(t *testing.T) {
	r := NewRing(50)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	nodes := r.GetReplicationNodes("somekey", 1)
	if len(nodes) != 1 {
		t.Errorf("factor=1 should return exactly 1 node, got %d", len(nodes))
	}
}

func TestGetReplicationNodesForHash_MaxHashWrapAround(t *testing.T) {
	r := NewRing(50)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	// ^uint32(0) is the max hash value, past all vnodes, so the ceiling loop
	// finds nothing and the continuation must wrap to the beginning of the ring.
	nodes := r.GetReplicationNodesForHash(^uint32(0), 2)
	if len(nodes) != 2 {
		t.Errorf("wrap-around should return 2 distinct nodes, got %d", len(nodes))
	}
}

func TestWalkRingHealthy_WrapAround(t *testing.T) {
	r := NewRing(50)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.SetHealthFilter(func(id string) bool { return true })

	// ^uint32(0) is past all vnode hashes; the ceiling loop finds nothing and
	// the continuation loop must wrap around to the start of the ring.
	r.mu.RLock()
	node, found := r.walkRingHealthy(^uint32(0))
	r.mu.RUnlock()

	if !found {
		t.Fatal("expected to find a healthy node after wrap-around")
	}
	if node == nil {
		t.Error("expected non-nil node")
	}
}

package ring

import (
	"fmt"
	"sort"
	"testing"
)

// lowHashLimit keeps all constructed vnodes in the bottom 1/16 of the hash
// space so a wrapping lookup target (above every vnode) is easy to find.
const lowHashLimit = uint32(1) << 28

// pickLowVnodeNodeIDs returns n node IDs whose replica-0 vnode hashes fall
// below lowHashLimit, sorted by vnode hash ascending.
func pickLowVnodeNodeIDs(t *testing.T, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; len(ids) < n && i < 1_000_000; i++ {
		id := fmt.Sprintf("node-%d", i)
		if computeHash(generateHashInput(id, 0)) < lowHashLimit {
			ids = append(ids, id)
		}
	}
	if len(ids) < n {
		t.Fatalf("could not find %d low-hash node IDs", n)
	}
	sort.Slice(ids, func(a, b int) bool {
		return computeHash(generateHashInput(ids[a], 0)) < computeHash(generateHashInput(ids[b], 0))
	})
	return ids
}

func TestGetReplicationNodesForHash_WrapStartsAtLowestVnode(t *testing.T) {
	// One vnode per node, all below lowHashLimit. A lookup hash above every
	// vnode must wrap and collect nodes starting from the lowest-hash vnode.
	ids := pickLowVnodeNodeIDs(t, 3)
	r := NewRing(1)
	for _, id := range ids {
		r.AddNode(id, id+":8080")
	}

	maxHash := computeHash(generateHashInput(ids[2], 0))
	nodes := r.GetReplicationNodesForHash(maxHash+1, 2)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].ID != ids[0] {
		t.Errorf("wrap-around primary: expected lowest-vnode node %s, got %s", ids[0], nodes[0].ID)
	}
	if nodes[1].ID != ids[1] {
		t.Errorf("wrap-around second replica: expected %s, got %s", ids[1], nodes[1].ID)
	}
}

func TestGetReplicationNodesForHash_WrapMatchesPrimaryRangeOwner(t *testing.T) {
	// The first node returned for a wrapping hash must agree with
	// GetPrimaryVnodeRanges: the lowest vnode owns the wrapping range.
	ids := pickLowVnodeNodeIDs(t, 3)
	r := NewRing(1)
	for _, id := range ids {
		r.AddNode(id, id+":8080")
	}

	maxHash := computeHash(generateHashInput(ids[2], 0))
	target := maxHash + 1

	owner := ""
	for _, id := range ids {
		for _, vr := range r.GetPrimaryVnodeRanges(id) {
			if vr.Contains(target) {
				owner = id
			}
		}
	}
	if owner == "" {
		t.Fatal("no primary vnode range contains the wrapping hash")
	}

	nodes := r.GetReplicationNodesForHash(target, 1)
	if len(nodes) != 1 || nodes[0].ID != owner {
		t.Errorf("replication walk primary %v disagrees with range owner %s", nodes, owner)
	}
}

func TestGetNode_HealthFilterWrapFindsLowestVnode(t *testing.T) {
	// Highest-vnode node is unhealthy; a key hashing above every vnode must
	// wrap to the lowest vnode's node, not skip it.
	ids := pickLowVnodeNodeIDs(t, 3)
	r := NewRing(1)
	for _, id := range ids {
		r.AddNode(id, id+":8080")
	}
	unhealthy := ids[2]
	r.SetHealthFilter(func(nodeID string) bool { return nodeID != unhealthy })

	maxHash := computeHash(generateHashInput(ids[2], 0))
	key := ""
	for i := 0; i < 1_000_000; i++ {
		candidate := fmt.Sprintf("key-%d", i)
		if computeHash(candidate) > maxHash {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("could not find a key hashing above the highest vnode")
	}

	node, ok := r.GetNode(key)
	if !ok {
		t.Fatal("expected a healthy node")
	}
	if node.ID != ids[0] {
		t.Errorf("expected wrap to lowest-vnode node %s, got %s", ids[0], node.ID)
	}
}

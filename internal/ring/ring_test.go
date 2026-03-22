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

	// Act + Assert — should not panic
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

	// Act — add a third node
	ring.AddNode("node3", "10.0.0.3")

	// Assert — only keys that moved to node3 should have changed
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

	// Assert — should return all available nodes, not panic
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
package ring

import (
	"fmt"
	"testing"
)

func TestNewTree(t *testing.T) {
	// Arrange + Act
	tree := NewTree()

	// Assert
	if tree == nil {
		t.Fatal("expected tree to not be nil")
	}
	if tree.Tree == nil {
		t.Fatal("expected inner tree to not be nil")
	}
}

func TestInsert(t *testing.T) {
	// Arrange
	tree := NewTree()
	node := NewNode("node1", "10.0.0.1")

	// Act
	tree.Insert(node, 3)

	// Assert
	if tree.Tree.Size() != 3 {
		t.Errorf("expected 3 vnodes, got %d", tree.Tree.Size())
	}
}

func TestInsertMultipleNodes(t *testing.T) {
	// Arrange
	tree := NewTree()
	node1 := NewNode("node1", "10.0.0.1")
	node2 := NewNode("node2", "10.0.0.2")

	// Act
	tree.Insert(node1, 3)
	tree.Insert(node2, 3)

	// Assert
	if tree.Tree.Size() != 6 {
		t.Errorf("expected 6 vnodes, got %d", tree.Tree.Size())
	}
}

func TestRemove(t *testing.T) {
	// Arrange
	tree := NewTree()
	node := NewNode("node1", "10.0.0.1")
	tree.Insert(node, 3)

	// Act
	tree.Remove(node, 3)

	// Assert
	if tree.Tree.Size() != 0 {
		t.Errorf("expected 0 vnodes, got %d", tree.Tree.Size())
	}
}

func TestNodeRemoveOnlyRemovesTargetNode(t *testing.T) {
	// Arrange
	tree := NewTree()
	node1 := NewNode("node1", "10.0.0.1")
	node2 := NewNode("node2", "10.0.0.2")
	tree.Insert(node1, 3)
	tree.Insert(node2, 3)

	// Act
	tree.Remove(node1, 3)

	// Assert
	if tree.Tree.Size() != 3 {
		t.Errorf("expected 3 vnodes, got %d", tree.Tree.Size())
	}
}

func TestGetNextReturnsCeilingVNode(t *testing.T) {
	// Arrange
	tree := NewTree()
	node := NewNode("node1", "10.0.0.1")
	tree.Insert(node, 150)

	// Act
	vnode, found := tree.GetNext(0)

	// Assert
	if !found {
		t.Fatal("expected to find a vnode")
	}
	if vnode.Node.ID != "node1" {
		t.Errorf("expected node1, got %s", vnode.Node.ID)
	}
}

func TestGetNextWrapsAround(t *testing.T) {
	// Arrange
	tree := NewTree()
	node := NewNode("node1", "10.0.0.1")
	tree.Insert(node, 150)

	// Act
	vnode, found := tree.GetNext(^uint32(0))

	// Assert
	if !found {
		t.Fatal("expected wraparound to return first vnode")
	}
	if vnode.Node.ID != "node1" {
		t.Errorf("expected node1, got %s", vnode.Node.ID)
	}
}

func TestGetNextEmptyTree(t *testing.T) {
	// Arrange
	tree := NewTree()

	// Act
	vnode, found := tree.GetNext(0)

	// Assert
	if found {
		t.Fatal("expected no vnode from empty tree")
	}
	if vnode != nil {
		t.Fatal("expected nil vnode from empty tree")
	}
}

func TestComputeHash(t *testing.T) {
	// Arrange
	key := "node1#0"

	// Act
	hash1 := computeHash(key)
	hash2 := computeHash(key)

	// Assert
	if hash1 != hash2 {
		t.Errorf("expected deterministic hash, got %d and %d", hash1, hash2)
	}
}

func TestComputeHashDifferentInputs(t *testing.T) {
	// Arrange
	key1 := "node1#0"
	key2 := "node1#1"

	// Act
	hash1 := computeHash(key1)
	hash2 := computeHash(key2)

	// Assert
	if hash1 == hash2 {
		t.Error("expected different hashes for different inputs")
	}
}

func TestGenerateHashInput(t *testing.T) {
	// Arrange + Act
	result := generateHashInput("node1", 5)

	// Assert
	if result != "node1#5" {
		t.Errorf("expected node1#5, got %s", result)
	}
}

func TestGenerateHashInputAllReplicas(t *testing.T) {
	// Arrange
	replicas := 3

	for i := 0; i < replicas; i++ {
		// Act
		result := generateHashInput("node1", i)

		// Assert
		expected := fmt.Sprintf("node1#%d", i)
		if result != expected {
			t.Errorf("expected %s, got %s", expected, result)
		}
	}
}
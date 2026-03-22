package ring

import "testing"

func TestNewNode(t *testing.T) {
	// Arrange
	id := "myNode"
	address := "1.2.3.4"

	// Act
	node := NewNode(id, address)

	// Assert
	if node.Address != address || node.ID != id {
		t.Errorf("Expected node %q with %q, got node with %q, %q", address, id, node.Address, node.ID)
	} 
}
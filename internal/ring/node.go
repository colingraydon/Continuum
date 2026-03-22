package ring

type Node struct {
	ID	string
	Address	string
}

func NewNode(id, address string) *Node {
	return &Node {
		ID:	id,
		Address: address,
	}
}
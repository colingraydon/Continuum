package ring

type Node struct {
	ID      string
	Address string
	// Zone is the failure domain (rack, availability zone, DC) this node
	// lives in. Replica placement spreads each key's replica set across
	// distinct zones when it can. Empty means unzoned: the node never
	// conflicts with any other node during placement.
	Zone string
}

func NewNode(id, address string) *Node {
	return &Node{
		ID:      id,
		Address: address,
	}
}

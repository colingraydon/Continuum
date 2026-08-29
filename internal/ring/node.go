package ring

type Node struct {
	ID      string
	Address string
	// DC is the data center this node lives in — the failure domain enclosing
	// Zone. It carries no placement meaning yet (multi-DC placement lands in a
	// later PR); today it is propagated and surfaced only. Empty means the
	// node's DC is unknown.
	DC string
	// Zone is the failure domain (rack, availability zone) this node lives in,
	// nested within DC. Replica placement spreads each key's replica set across
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

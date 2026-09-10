package ring

type Node struct {
	ID      string
	Address string
	// DC is the data center this node lives in — the failure domain enclosing
	// Zone. With a per-DC replica table installed (SetDCReplication) it drives
	// placement: a key keeps that DC's configured number of replicas here.
	// Empty means the node's DC is unknown.
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

package gossip

import "testing"

func TestPeerGossipAddrPrefersAdvertised(t *testing.T) {
	g := &Gossiper{selfID: "self", gossipPort: "9000"}
	m := &Member{ID: "peer", Address: "10.0.0.5:8080", GossipAddr: "10.0.0.5:9555"}
	if got := g.peerGossipAddr(m); got != "10.0.0.5:9555" {
		t.Errorf("expected advertised gossip addr, got %q", got)
	}
}

func TestPeerGossipAddrFallsBackToSelfPort(t *testing.T) {
	g := &Gossiper{selfID: "self", gossipPort: "9000"}
	m := &Member{ID: "peer", Address: "10.0.0.5:8080"}
	if got := g.peerGossipAddr(m); got != "10.0.0.5:9000" {
		t.Errorf("expected fallback to self gossip port, got %q", got)
	}
}

func TestAddWithGossipStoresAddr(t *testing.T) {
	ml := NewMemberList("self", "localhost:8080", nil)
	ml.AddWithGossip("peer", "10.0.0.5:8080", "10.0.0.5:9555")
	m, ok := ml.Get("peer")
	if !ok {
		t.Fatal("peer not found after AddWithGossip")
	}
	if m.GossipAddr != "10.0.0.5:9555" {
		t.Errorf("expected gossip addr stored, got %q", m.GossipAddr)
	}
}

func TestAddLeavesGossipAddrEmpty(t *testing.T) {
	ml := NewMemberList("self", "localhost:8080", nil)
	ml.Add("peer", "10.0.0.5:8080")
	m, _ := ml.Get("peer")
	if m.GossipAddr != "" {
		t.Errorf("expected empty gossip addr from Add, got %q", m.GossipAddr)
	}
}

func TestSetSelfGossipAddrPropagates(t *testing.T) {
	ml := NewMemberList("self", "localhost:8080", nil)
	self, _ := ml.Get("self")
	before := self.Heartbeat

	ml.SetSelfGossipAddr("127.0.0.1:9555")

	self, _ = ml.Get("self")
	if self.GossipAddr != "127.0.0.1:9555" {
		t.Errorf("expected self gossip addr set, got %q", self.GossipAddr)
	}
	if self.Heartbeat <= before {
		t.Error("expected heartbeat bump so the change gossips to peers")
	}
}

func TestMergePropagatesGossipAddr(t *testing.T) {
	ml := NewMemberList("self", "localhost:8080", nil)
	ml.Merge([]*Member{{ID: "peer", Address: "10.0.0.5:8080", GossipAddr: "10.0.0.5:9555", Heartbeat: 3}})
	m, ok := ml.Get("peer")
	if !ok {
		t.Fatal("peer not merged")
	}
	if m.GossipAddr != "10.0.0.5:9555" {
		t.Errorf("expected gossip addr to survive merge, got %q", m.GossipAddr)
	}
}

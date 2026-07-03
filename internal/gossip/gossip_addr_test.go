package gossip

import (
	"net"
	"testing"
	"time"
)

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

// newReceiverTransport starts a transport on an ephemeral port and returns it
// with its loopback address, so senders can target it explicitly.
func newReceiverTransport(t *testing.T) (*Transport, string) {
	t.Helper()
	tr, err := NewTransport("0")
	if err != nil {
		t.Fatalf("receiver transport: %v", err)
	}
	tr.Start()
	t.Cleanup(tr.Stop)
	_, port, err := net.SplitHostPort(tr.conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("receiver addr: %v", err)
	}
	return tr, "127.0.0.1:" + port
}

func waitForMessage(t *testing.T, tr *Transport, from string) *GossipMessage {
	t.Helper()
	select {
	case msg := <-tr.Incoming():
		if msg.From != from {
			t.Fatalf("expected message from %q, got %q", from, msg.From)
		}
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("no gossip message received within 3s")
		return nil
	}
}

// TestGossipRoundSendsToAdvertisedAddr proves a full round delivers datagrams
// to a peer's advertised gossip address (not the sender's own port), and that
// a peer with an unresolvable address only logs rather than aborting the round.
func TestGossipRoundSendsToAdvertisedAddr(t *testing.T) {
	receiver, receiverAddr := newReceiverTransport(t)

	ml := newTestMemberList()
	// Bogus HTTP address: only the advertised gossip address can succeed.
	ml.AddWithGossip("peer-good", "10.255.255.1:1", receiverAddr)
	// Unresolvable gossip address exercises the send-error branch.
	ml.AddWithGossip("peer-bad", "10.255.255.2:1", "not-a-valid-address")

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("gossiper: %v", err)
	}
	defer transport.Stop()

	g.gossipRound()

	msg := waitForMessage(t, receiver, "self")
	if msg.Type != MessagePushPull {
		t.Errorf("expected push-pull message, got %v", msg.Type)
	}
}

// TestNotifyDeadSendsToAdvertisedAddr proves the shutdown broadcast reaches
// peers via their advertised gossip addresses and carries self marked dead.
func TestNotifyDeadSendsToAdvertisedAddr(t *testing.T) {
	receiver, receiverAddr := newReceiverTransport(t)

	ml := newTestMemberList()
	ml.AddWithGossip("peer-good", "10.255.255.1:1", receiverAddr)
	ml.AddWithGossip("peer-bad", "10.255.255.2:1", "not-a-valid-address")

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("gossiper: %v", err)
	}
	defer transport.Stop()

	g.NotifyDead()

	msg := waitForMessage(t, receiver, "self")
	for _, m := range msg.Members {
		if m.ID == "self" {
			if m.Status != MemberDead {
				t.Errorf("expected self marked dead in broadcast, got %v", m.Status)
			}
			return
		}
	}
	t.Error("broadcast did not include self")
}

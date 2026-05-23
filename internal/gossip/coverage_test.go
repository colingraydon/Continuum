package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestGossipRound_WithPeers(t *testing.T) {
	// Set up a receiver so we can observe the outbound gossip message.
	receiver := newTestTransport(t)
	receiver.Start()
	port := fmt.Sprintf("%d", receiver.conn.LocalAddr().(*net.UDPAddr).Port)

	ml := newTestMemberList()
	senderTransport, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer senderTransport.Stop()

	// Use receiver's port as gossipPort so all peer sends land on the receiver.
	g := NewGossiper("self", port, ml, senderTransport)
	ml.Add("node1", "127.0.0.1:8080") // actual port is overridden by gossipPort

	g.gossipRound()

	select {
	case msg := <-receiver.Incoming():
		if msg.From != "self" {
			t.Errorf("expected from=self, got %s", msg.From)
		}
		if msg.Type != MessagePushPull {
			t.Errorf("expected PushPull, got %d", msg.Type)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout: no gossip message received")
	}
}

func TestBootstrap_NoSeeds(t *testing.T) {
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	g.Bootstrap(nil)
	g.Bootstrap([]string{})
}

func TestBootstrap_SendsToSeeds(t *testing.T) {
	receiver := newTestTransport(t)
	receiver.Start()
	port := fmt.Sprintf("%d", receiver.conn.LocalAddr().(*net.UDPAddr).Port)

	ml := newTestMemberList()
	senderTransport, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer senderTransport.Stop()

	g := NewGossiper("self", port, ml, senderTransport)

	// The seed address host is used; port is replaced by gossipPort.
	g.Bootstrap([]string{"127.0.0.1:9999"})

	select {
	case msg := <-receiver.Incoming():
		if msg.From != "self" {
			t.Errorf("expected from=self, got %s", msg.From)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout: no bootstrap message received")
	}
}

func TestBootstrap_LogsOnSendFailure(t *testing.T) {
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// An invalid address causes Send to fail; Bootstrap should log and continue.
	g.Bootstrap([]string{"invalid-host:notaport"})
}

func TestGossipHost_NoPort(t *testing.T) {
	// An address without a port is returned as-is.
	got := gossipHost("10.0.0.1")
	if got != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1, got %s", got)
	}
}

func TestGossipHost_WithPort(t *testing.T) {
	got := gossipHost("10.0.0.1:8080")
	if got != "10.0.0.1" {
		t.Errorf("expected 10.0.0.1, got %s", got)
	}
}

func TestMarkSuspect_NonAliveMember(t *testing.T) {
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkDead("node1")

	// MarkSuspect on a dead member should be a no-op (Status != MemberAlive).
	before := func() MemberStatus {
		m, _ := ml.Get("node1")
		return m.Status
	}()
	ml.MarkSuspect("node1")
	after, _ := ml.Get("node1")
	if after.Status != before {
		t.Errorf("MarkSuspect on dead node should be a no-op, was %s", after.Status)
	}
}

func TestMarkSuspect_UnknownMember(t *testing.T) {
	ml := newTestMemberList()
	ml.MarkSuspect("ghost") // must not panic
}

func TestMarkDead_AlreadyDead(t *testing.T) {
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkDead("node1")

	callCount := 0
	ml.onChange = func(m *Member, s MemberStatus) { callCount++ }
	ml.MarkDead("node1") // no-op: already dead

	if callCount != 0 {
		t.Errorf("MarkDead on already-dead member should not trigger onChange, got %d calls", callCount)
	}
}

func TestMarkDead_UnknownMember(t *testing.T) {
	ml := newTestMemberList()
	ml.MarkDead("ghost") // must not panic
}

func TestMarkSuspect_AliveWithOnChange(t *testing.T) {
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	called := false
	ml.onChange = func(m *Member, s MemberStatus) {
		if s == MemberSuspect {
			called = true
		}
	}
	ml.MarkSuspect("node1")
	if !called {
		t.Error("onChange should be called when marking alive node suspect")
	}
}

func TestMarkDead_AliveWithOnChange(t *testing.T) {
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	called := false
	ml.onChange = func(m *Member, s MemberStatus) {
		if s == MemberDead {
			called = true
		}
	}
	ml.MarkDead("node1")
	if !called {
		t.Error("onChange should be called when marking alive node dead")
	}
}

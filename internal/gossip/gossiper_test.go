package gossip

import (
	"context"
	"testing"
	"time"
	"fmt"
)

func newTestGossiper(selfID string, ml *MemberList) (*Gossiper, *Transport, error) {
	t, err := NewTransport("0")
	if err != nil {
		return nil, nil, err
	}
	g := NewGossiper(selfID, "0", ml, t)
	return g, t, nil
}

func TestNewGossiper(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Assert
	if g == nil {
		t.Fatal("expected gossiper to not be nil")
	}
}

func TestSelectPeersEmpty(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	peers := g.selectPeers()

	// Assert
	if len(peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(peers))
	}
}

func TestSelectPeersExcludesSelf(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.Add("node2", "10.0.0.3")
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	peers := g.selectPeers()

	// Assert
	for _, p := range peers {
		if p.ID == "self" {
			t.Error("expected self to be excluded from peers")
		}
	}
}

func TestSelectPeersCappedAtFanout(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	for i := 0; i < 10; i++ {
		ml.Add(fmt.Sprintf("node%d", i), fmt.Sprintf("10.0.0.%d", i))
	}
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	peers := g.selectPeers()

	// Assert
	if len(peers) > fanout {
		t.Errorf("expected at most %d peers, got %d", fanout, len(peers))
	}
}

func TestSelectPeersExcludesDead(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkDead("node1")
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	peers := g.selectPeers()

	// Assert
	if len(peers) != 0 {
		t.Errorf("expected 0 peers after dead node, got %d", len(peers))
	}
}

func TestHandleMessageMergesMembers(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	msg := &GossipMessage{
		Type: MessagePushPull,
		From: "node1",
		Members: []*Member{
			{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, UpdatedAt: time.Now(), Status: MemberAlive},
		},
	}

	// Act
	g.handleMessage(msg)

	// Assert
	if ml.Size() != 2 {
		t.Errorf("expected 2 members after merge, got %d", ml.Size())
	}
}


func TestCheckStaleMarksSuspect(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	// manually set UpdatedAt to past stale threshold
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			m.UpdatedAt = time.Now().Add(-10 * time.Second)
		}
	}

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	g.checkStale()

	// Assert
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			if m.Status != MemberSuspect {
				t.Errorf("expected suspect, got %s", m.Status)
			}
			return
		}
	}
}

func TestCheckStaleMarksDead(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkSuspect("node1")

	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			m.UpdatedAt = time.Now().Add(-10 * time.Second)
		}
	}

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	g.checkStale()

	// Assert
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			if m.Status != MemberDead {
				t.Errorf("expected dead, got %s", m.Status)
			}
			return
		}
	}
}

func TestCheckStaleSkipsSelf(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	for _, m := range ml.GetAll() {
		if m.ID == "self" {
			m.UpdatedAt = time.Now().Add(-10 * time.Second)
		}
	}

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	g.checkStale()

	// Assert — self should still be alive
	for _, m := range ml.GetAll() {
		if m.ID == "self" {
			if m.Status != MemberAlive {
				t.Errorf("expected self to stay alive, got %s", m.Status)
			}
			return
		}
	}
}

func TestCheckStaleSkipsDead(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkDead("node1")

	callCount := 0
	ml.onChange = func(m *Member, s MemberStatus) {
		callCount++
	}

	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			m.UpdatedAt = time.Now().Add(-10 * time.Second)
		}
	}

	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	g.checkStale()

	// Assert — no additional status change for already dead node
	if callCount != 0 {
		t.Errorf("expected no callbacks for dead node, got %d", callCount)
	}
}

func TestStartAndStop(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	// Act + Assert — should not panic
	g.Start(ctx)
	cancel()
	g.Stop()
}

func TestGossipRoundIncrementsHeartbeat(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	// Act
	g.gossipRound()

	// Assert
	for _, m := range ml.GetAll() {
		if m.ID == "self" {
			if m.Heartbeat != 1 {
				t.Errorf("expected heartbeat 1, got %d", m.Heartbeat)
			}
			return
		}
	}
}
package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// getMember is a small test helper to fetch a member by id or fail.
func getMember(t *testing.T, ml *MemberList, id string) *Member {
	t.Helper()
	m, ok := ml.Get(id)
	if !ok {
		t.Fatalf("expected member %q to exist", id)
	}
	return m
}

// TestMergeHigherIncarnationWinsDespiteLowerHeartbeat is the crash-rejoin case:
// a node comes back with a fresh (higher) incarnation but its heartbeat reset
// to near zero. Incarnation must dominate so peers accept the fresh state
// immediately instead of waiting for the heartbeat to out-count the stale one.
func TestMergeHigherIncarnationWinsDespiteLowerHeartbeat(t *testing.T) {
	ml := newTestMemberList()
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 5, Heartbeat: 500, Status: MemberAlive},
	})

	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 6, Heartbeat: 1, Status: MemberAlive},
	})

	m := getMember(t, ml, "node1")
	if m.Incarnation != 6 || m.Heartbeat != 1 {
		t.Errorf("expected higher incarnation to win: got inc=%d hb=%d, want inc=6 hb=1", m.Incarnation, m.Heartbeat)
	}
}

// TestMergeLowerIncarnationRejectedDespiteHigherHeartbeat guards the inverse:
// a stale view at an older incarnation must not overwrite a newer one even if
// its heartbeat is far ahead.
func TestMergeLowerIncarnationRejectedDespiteHigherHeartbeat(t *testing.T) {
	ml := newTestMemberList()
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 5, Heartbeat: 1, Status: MemberAlive},
	})

	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 4, Heartbeat: 999, Status: MemberDead},
	})

	m := getMember(t, ml, "node1")
	if m.Incarnation != 5 || m.Status != MemberAlive {
		t.Errorf("expected stale lower-incarnation view to be rejected: got inc=%d status=%s", m.Incarnation, m.Status)
	}
}

// TestMergeSameIncarnationHigherHeartbeatWins confirms heartbeat still breaks
// ties within an incarnation, preserving the pre-existing liveness signal.
func TestMergeSameIncarnationHigherHeartbeatWins(t *testing.T) {
	ml := newTestMemberList()
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 3, Heartbeat: 5, Status: MemberAlive},
	})

	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Incarnation: 3, Heartbeat: 10, Status: MemberAlive},
	})

	if m := getMember(t, ml, "node1"); m.Heartbeat != 10 {
		t.Errorf("expected heartbeat 10 to win at equal incarnation, got %d", m.Heartbeat)
	}
}

// TestRefuteSelfBumpsPastStaleIncarnation covers the core rejoin mechanism:
// peers remember a higher incarnation (and have buried us), so we must advance
// past it and stay alive.
func TestRefuteSelfBumpsPastStaleIncarnation(t *testing.T) {
	ml := newTestMemberList() // self starts at incarnation 0
	ml.Merge([]*Member{
		{ID: "self", Address: "10.0.0.1", Incarnation: 5, Heartbeat: 500, Status: MemberDead},
	})

	self := getMember(t, ml, "self")
	if self.Incarnation != 6 {
		t.Errorf("expected self to refute to incarnation 6, got %d", self.Incarnation)
	}
	if self.Status != MemberAlive {
		t.Errorf("expected self to remain alive after refutation, got %s", self.Status)
	}
}

// TestRefuteSelfRefutesSuspectAtSameIncarnation: a peer suspecting us at our
// current incarnation must be overridden by bumping one past it.
func TestRefuteSelfRefutesSuspectAtSameIncarnation(t *testing.T) {
	ml := newTestMemberList()
	ml.self.Incarnation = 3

	ml.Merge([]*Member{
		{ID: "self", Address: "10.0.0.1", Incarnation: 3, Heartbeat: 9, Status: MemberSuspect},
	})

	if ml.self.Incarnation != 4 {
		t.Errorf("expected self to refute suspect claim to incarnation 4, got %d", ml.self.Incarnation)
	}
}

// TestRefuteSelfNoopOnAliveEcho: peers routinely echo our own alive state back
// at our current incarnation. That must not bump the incarnation, or it would
// grow without bound every round.
func TestRefuteSelfNoopOnAliveEcho(t *testing.T) {
	ml := newTestMemberList()
	ml.self.Incarnation = 3
	ml.self.Heartbeat = 2

	ml.Merge([]*Member{
		{ID: "self", Address: "10.0.0.1", Incarnation: 3, Heartbeat: 99, Status: MemberAlive},
	})

	if ml.self.Incarnation != 3 {
		t.Errorf("expected no bump on alive echo, got incarnation %d", ml.self.Incarnation)
	}
	if ml.self.Heartbeat != 2 {
		t.Errorf("expected heartbeat untouched by refutation, got %d", ml.self.Heartbeat)
	}
}

// TestHandleMessageRepliesToBootstrap verifies the receiver answers a
// WantReply message by pushing its member list back to the sender's advertised
// gossip address, and that the reply itself does not ask for a reply (no loop).
func TestHandleMessageRepliesToBootstrap(t *testing.T) {
	// The "rejoining" node's transport — this is where the reply must land.
	rejoiner := newTestTransport(t)
	rejoiner.Start()
	rejoinerAddr := fmt.Sprintf("127.0.0.1:%d", rejoiner.conn.LocalAddr().(*net.UDPAddr).Port)

	// The seed node A, holding the rejoiner as dead so it would not otherwise
	// gossip to it.
	mlA := NewMemberList("A", "127.0.0.1:8080", nil)
	senderTransport, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer senderTransport.Stop()
	gA := NewGossiper("A", "0", mlA, senderTransport)
	mlA.AddWithGossip("B", "127.0.0.1:9090", rejoinerAddr)
	mlA.MarkDead("B")

	bootstrap := &GossipMessage{
		Type:      MessagePushPull,
		From:      "B",
		WantReply: true,
		Members: []*Member{
			{ID: "B", Address: "127.0.0.1:9090", GossipAddr: rejoinerAddr, Incarnation: 0, Heartbeat: 0, Status: MemberAlive},
		},
	}

	gA.handleMessage(bootstrap)

	select {
	case reply := <-rejoiner.Incoming():
		if reply.From != "A" {
			t.Errorf("expected reply from A, got %s", reply.From)
		}
		if reply.WantReply {
			t.Error("reply must not set WantReply, or it would loop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: bootstrap did not receive a reply")
	}
}

// TestHandleMessageNoReplyWithoutWantReply confirms ordinary gossip does not
// trigger a reply, so steady-state traffic is unchanged.
func TestHandleMessageNoReplyWithoutWantReply(t *testing.T) {
	rejoiner := newTestTransport(t)
	rejoiner.Start()
	rejoinerAddr := fmt.Sprintf("127.0.0.1:%d", rejoiner.conn.LocalAddr().(*net.UDPAddr).Port)

	mlA := NewMemberList("A", "127.0.0.1:8080", nil)
	senderTransport, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer senderTransport.Stop()
	gA := NewGossiper("A", "0", mlA, senderTransport)

	msg := &GossipMessage{
		Type: MessagePushPull,
		From: "B",
		Members: []*Member{
			{ID: "B", Address: "127.0.0.1:9090", GossipAddr: rejoinerAddr, Heartbeat: 1, Status: MemberAlive},
		},
	}

	gA.handleMessage(msg)

	select {
	case <-rejoiner.Incoming():
		t.Error("expected no reply for a message without WantReply")
	case <-time.After(200 * time.Millisecond):
		// expected
	}
}

// TestSenderGossipAddrUnknownSender returns empty when we can resolve no
// address for the sender, so replyTo becomes a no-op instead of sending blind.
func TestSenderGossipAddrUnknownSender(t *testing.T) {
	ml := newTestMemberList()
	g, transport, err := newTestGossiper("self", ml)
	if err != nil {
		t.Fatalf("failed to create gossiper: %v", err)
	}
	defer transport.Stop()

	addr := g.senderGossipAddr(&GossipMessage{From: "ghost", Members: nil})
	if addr != "" {
		t.Errorf("expected empty address for unknown sender, got %q", addr)
	}
}

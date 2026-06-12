package gossip

import (
	"testing"
	"time"
)

func TestMergeStampsLocalUpdatedAt(t *testing.T) {
	// The wire UpdatedAt is the sender's wall clock. If Merge stored it
	// verbatim, clock skew or a stale relayed timestamp would let the stale
	// checker mark a member suspect immediately after accepting a fresh
	// heartbeat. Merge must stamp accepted updates with the local clock.
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	stale := time.Now().Add(-time.Hour)
	incoming := []*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 10, UpdatedAt: stale, Status: MemberAlive},
	}

	before := time.Now()
	ml.Merge(incoming)

	m, ok := ml.Get("node1")
	if !ok {
		t.Fatal("expected node1 to exist")
	}
	if m.Heartbeat != 10 {
		t.Fatalf("expected merge to accept heartbeat 10, got %d", m.Heartbeat)
	}
	if m.UpdatedAt.Before(before) {
		t.Errorf("expected UpdatedAt to be stamped locally at merge time, got %v", m.UpdatedAt)
	}
}

func TestMergeStampsLocalUpdatedAtForNewMember(t *testing.T) {
	ml := newTestMemberList()

	stale := time.Now().Add(-time.Hour)
	before := time.Now()
	ml.Merge([]*Member{
		{ID: "node9", Address: "10.0.0.9", Heartbeat: 1, UpdatedAt: stale, Status: MemberAlive},
	})

	m, ok := ml.Get("node9")
	if !ok {
		t.Fatal("expected node9 to exist")
	}
	if m.UpdatedAt.Before(before) {
		t.Errorf("expected UpdatedAt to be stamped locally at merge time, got %v", m.UpdatedAt)
	}
}

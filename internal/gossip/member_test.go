// internal/gossip/member_test.go
package gossip

import (
	"testing"
	"time"
)

func newTestMemberList() *MemberList {
	return NewMemberList("self", "10.0.0.1", nil)
}

func TestNewMemberList(t *testing.T) {
	// Arrange + Act
	ml := newTestMemberList()

	// Assert
	if ml == nil {
		t.Fatal("expected member list to not be nil")
	}
	if ml.Size() != 1 {
		t.Errorf("expected 1 member (self), got %d", ml.Size())
	}
}

func TestMemberStatusString(t *testing.T) {
	// Arrange + Act + Assert
	if MemberAlive.String() != "alive" {
		t.Errorf("expected alive, got %s", MemberAlive.String())
	}
	if MemberSuspect.String() != "suspect" {
		t.Errorf("expected suspect, got %s", MemberSuspect.String())
	}
	if MemberDead.String() != "dead" {
		t.Errorf("expected dead, got %s", MemberDead.String())
	}
	if MemberStatus(99).String() != "unknown" {
		t.Errorf("expected unknown, got %s", MemberStatus(99).String())
	}
}

func TestIncrementHeartbeat(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act
	ml.IncrementHeartbeat()

	// Assert
	members := ml.GetAll()
	for _, m := range members {
		if m.ID == "self" {
			if m.Heartbeat != 1 {
				t.Errorf("expected heartbeat 1, got %d", m.Heartbeat)
			}
			return
		}
	}
	t.Fatal("self not found in member list")
}

func TestIncrementHeartbeatUpdatesTime(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	before := time.Now()

	// Act
	ml.IncrementHeartbeat()

	// Assert
	members := ml.GetAll()
	for _, m := range members {
		if m.ID == "self" {
			if m.UpdatedAt.Before(before) {
				t.Error("expected UpdatedAt to be updated after heartbeat increment")
			}
			return
		}
	}
}

func TestAdd(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act
	ml.Add("node1", "10.0.0.2")

	// Assert
	if ml.Size() != 2 {
		t.Errorf("expected 2 members, got %d", ml.Size())
	}
}

func TestAddSetsStatusAlive(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act
	ml.Add("node1", "10.0.0.2")

	// Assert
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			if m.Status != MemberAlive {
				t.Errorf("expected alive, got %s", m.Status)
			}
			return
		}
	}
	t.Fatal("node1 not found")
}

func TestGetAll(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.Add("node2", "10.0.0.3")

	// Act
	members := ml.GetAll()

	// Assert
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}
}

func TestGetAlive(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.Add("node2", "10.0.0.3")
	ml.MarkDead("node2")

	// Act
	alive := ml.GetAlive()

	// Assert
	if len(alive) != 2 {
		t.Errorf("expected 2 alive members, got %d", len(alive))
	}
}

func TestMarkSuspect(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	// Act
	ml.MarkSuspect("node1")

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

func TestMarkSuspectNoopIfAlreadySuspect(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkSuspect("node1")

	callCount := 0
	ml.onChange = func(m *Member, s MemberStatus) {
		callCount++
	}

	// Act
	ml.MarkSuspect("node1")

	// Assert
	if callCount != 0 {
		t.Errorf("expected no callback, got %d", callCount)
	}
}

func TestMarkDead(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	// Act
	ml.MarkDead("node1")

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

func TestMarkDeadNoopIfAlreadyDead(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.MarkDead("node1")

	callCount := 0
	ml.onChange = func(m *Member, s MemberStatus) {
		callCount++
	}

	// Act
	ml.MarkDead("node1")

	// Assert
	if callCount != 0 {
		t.Errorf("expected no callback, got %d", callCount)
	}
}

func TestMarkSuspectNonExistent(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act + Assert - should not panic
	ml.MarkSuspect("nonexistent")
}

func TestMarkDeadNonExistent(t *testing.T) {
	// Arrange
	ml := newTestMemberList()

	// Act + Assert - should not panic
	ml.MarkDead("nonexistent")
}

func TestMergeAddsNewMember(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	incoming := []*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, UpdatedAt: time.Now(), Status: MemberAlive},
	}

	// Act
	ml.Merge(incoming)

	// Assert
	if ml.Size() != 2 {
		t.Errorf("expected 2 members, got %d", ml.Size())
	}
}

func TestMergeKeepsHighestHeartbeat(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")

	incoming := []*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 10, UpdatedAt: time.Now(), Status: MemberAlive},
	}

	// Act
	ml.Merge(incoming)

	// Assert
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			if m.Heartbeat != 10 {
				t.Errorf("expected heartbeat 10, got %d", m.Heartbeat)
			}
			return
		}
	}
}

func TestMergeIgnoresLowerHeartbeat(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 10, UpdatedAt: time.Now(), Status: MemberAlive},
	})

	// Act - merge with lower heartbeat
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 5, UpdatedAt: time.Now(), Status: MemberAlive},
	})

	// Assert - heartbeat should still be 10
	for _, m := range ml.GetAll() {
		if m.ID == "node1" {
			if m.Heartbeat != 10 {
				t.Errorf("expected heartbeat 10, got %d", m.Heartbeat)
			}
			return
		}
	}
}

func TestMergeIgnoresSelf(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.IncrementHeartbeat()

	incoming := []*Member{
		{ID: "self", Address: "10.0.0.1", Heartbeat: 999, UpdatedAt: time.Now(), Status: MemberAlive},
	}

	// Act
	ml.Merge(incoming)

	// Assert - self heartbeat should still be 1
	for _, m := range ml.GetAll() {
		if m.ID == "self" {
			if m.Heartbeat != 1 {
				t.Errorf("expected heartbeat 1, got %d", m.Heartbeat)
			}
			return
		}
	}
}

func TestMergeFiresOnChangeForNewMember(t *testing.T) {
	// Arrange
	var calledWith MemberStatus
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		calledWith = s
	})

	// Act
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, UpdatedAt: time.Now(), Status: MemberAlive},
	})

	// Assert
	if calledWith != MemberAlive {
		t.Errorf("expected alive callback, got %s", calledWith)
	}
}

func TestMergeFiresOnChangeForStatusChange(t *testing.T) {
	// Arrange
	var calledWith MemberStatus
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		calledWith = s
	})
	ml.Add("node1", "10.0.0.2")

	// Act - merge with dead status and higher heartbeat
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 10, UpdatedAt: time.Now(), Status: MemberDead},
	})

	// Assert
	if calledWith != MemberDead {
		t.Errorf("expected dead callback, got %s", calledWith)
	}
}

func TestSize(t *testing.T) {
	// Arrange
	ml := newTestMemberList()
	ml.Add("node1", "10.0.0.2")
	ml.Add("node2", "10.0.0.3")

	// Act
	size := ml.Size()

	// Assert
	if size != 3 {
		t.Errorf("expected 3, got %d", size)
	}
}
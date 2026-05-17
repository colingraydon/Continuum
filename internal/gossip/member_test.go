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

func TestSetBootstrapping_SetsFlag(t *testing.T) {
	ml := newTestMemberList()
	ml.SetBootstrapping("self", true)
	m, ok := ml.Get("self")
	if !ok || !m.Bootstrapping {
		t.Fatal("expected Bootstrapping=true after SetBootstrapping(true)")
	}
}

func TestSetBootstrapping_IncrementsHeartbeat(t *testing.T) {
	ml := newTestMemberList()
	before := ml.self.Heartbeat
	ml.SetBootstrapping("self", true)
	if ml.self.Heartbeat != before+1 {
		t.Errorf("expected heartbeat %d, got %d", before+1, ml.self.Heartbeat)
	}
}

func TestSetBootstrapping_NoopIfSameValue(t *testing.T) {
	ml := newTestMemberList()
	ml.SetBootstrapping("self", false) // already false
	if ml.self.Heartbeat != 0 {
		t.Error("expected no heartbeat increment when value unchanged")
	}
}

func TestSetBootstrapping_FalseTriggersCallback(t *testing.T) {
	var fired MemberStatus = -1
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		fired = s
	})
	ml.SetBootstrapping("self", true)
	fired = -1 // reset — setting true does not fire
	ml.SetBootstrapping("self", false)
	if fired != MemberBootstrapped {
		t.Errorf("expected MemberBootstrapped callback, got %v", fired)
	}
}

func TestSetBootstrapping_TrueDoesNotTriggerCallback(t *testing.T) {
	called := false
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		called = true
	})
	ml.SetBootstrapping("self", true)
	if called {
		t.Error("expected no callback when setting bootstrapping=true")
	}
}

func TestMerge_BootstrappingPropagates(t *testing.T) {
	ml := newTestMemberList()
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, Status: MemberAlive, Bootstrapping: true},
	})
	m, ok := ml.Get("node1")
	if !ok || !m.Bootstrapping {
		t.Fatal("expected Bootstrapping=true to propagate via Merge")
	}
}

func TestMerge_BootstrappedCallbackFiresOnTransition(t *testing.T) {
	var fired MemberStatus = -1
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		fired = s
	})
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, Status: MemberAlive, Bootstrapping: true},
	})
	fired = -1
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 2, Status: MemberAlive, Bootstrapping: false},
	})
	if fired != MemberBootstrapped {
		t.Errorf("expected MemberBootstrapped on bootstrapping transition, got %v", fired)
	}
}

func TestMerge_NoBootstrappedCallbackWithoutTransition(t *testing.T) {
	var fired MemberStatus = -1
	ml := NewMemberList("self", "10.0.0.1", func(m *Member, s MemberStatus) {
		fired = s
	})
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, Status: MemberAlive, Bootstrapping: false},
	})
	fired = -1
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 2, Status: MemberAlive, Bootstrapping: false},
	})
	if fired == MemberBootstrapped {
		t.Error("expected no MemberBootstrapped callback when bootstrapping was already false")
	}
}

func TestMemberBootstrappedString(t *testing.T) {
	if MemberBootstrapped.String() != "bootstrapped" {
		t.Errorf("expected 'bootstrapped', got %q", MemberBootstrapped.String())
	}
}

func TestSetSelfWeight_UpdatesWeight(t *testing.T) {
	ml := newTestMemberList()
	ml.SetSelfWeight(2.0)
	if ml.self.Weight != 2.0 {
		t.Errorf("expected weight 2.0, got %f", ml.self.Weight)
	}
}

func TestSetSelfWeight_IncrementsHeartbeat(t *testing.T) {
	ml := newTestMemberList()
	before := ml.self.Heartbeat
	ml.SetSelfWeight(2.0)
	if ml.self.Heartbeat != before+1 {
		t.Errorf("expected heartbeat %d, got %d", before+1, ml.self.Heartbeat)
	}
}

func TestMerge_WeightPropagates(t *testing.T) {
	ml := newTestMemberList()
	ml.Merge([]*Member{
		{ID: "node1", Address: "10.0.0.2", Heartbeat: 1, Status: MemberAlive, Weight: 2.5},
	})
	m, ok := ml.Get("node1")
	if !ok {
		t.Fatal("node1 not found after merge")
	}
	if m.Weight != 2.5 {
		t.Errorf("expected weight 2.5, got %f", m.Weight)
	}
}

func TestNewMemberList_SelfWeightDefaultsToOne(t *testing.T) {
	ml := newTestMemberList()
	if ml.self.Weight != 1.0 {
		t.Errorf("expected default self weight 1.0, got %f", ml.self.Weight)
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
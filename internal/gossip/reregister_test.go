package gossip

import "testing"

// TestAddWithGossip_SurvivesStaleDeadEntry is the regression for a ring
// convergence livelock. Incarnation is the primary precedence key in Merge, so
// registering a member at incarnation 0 produced the weakest possible entry:
// the next gossip round carrying the peer's older dead view (at a higher
// incarnation) superseded it and evicted the node from the ring again. An
// explicit re-registration must outrank the state it is correcting.
func TestAddWithGossip_SurvivesStaleDeadEntry(t *testing.T) {
	// Arrange: a peer has told us n2 is dead at incarnation 3.
	var deaths []string
	ml := NewMemberList("self", "self:80", func(m *Member, s MemberStatus) {
		if s == MemberDead {
			deaths = append(deaths, m.ID)
		}
	})
	staleDead := func() []*Member {
		return []*Member{{ID: "n2", Address: "n2:80", Incarnation: 3, Status: MemberDead}}
	}
	ml.Merge(staleDead())

	// Act: re-register n2 explicitly, as POST /nodes does, then let the same
	// stale entry arrive again from a peer that has not heard about it yet.
	ml.AddWithGossip("n2", "n2:80", "n2:81")
	deaths = nil
	ml.Merge(staleDead())

	// Assert
	m, ok := ml.Get("n2")
	if !ok {
		t.Fatal("n2 missing from the member list")
	}
	if m.Status != MemberAlive {
		t.Errorf("n2 status = %v, want alive — the stale dead entry won", m.Status)
	}
	if len(deaths) != 0 {
		t.Errorf("re-registration was reverted, firing MemberDead for %v", deaths)
	}
}

func TestAddWithGossip_AdvancesPastNonAliveIncarnation(t *testing.T) {
	// A suspect entry is corrected the same way a dead one is: the
	// registration has to outrank the claim it contradicts.
	ml := NewMemberList("self", "self:80", nil)
	ml.Merge([]*Member{{ID: "n2", Address: "n2:80", Incarnation: 7, Status: MemberSuspect}})

	ml.AddWithGossip("n2", "n2:80", "")

	m, _ := ml.Get("n2")
	if m.Incarnation != 8 {
		t.Errorf("incarnation = %d, want 8 (one past the suspect claim)", m.Incarnation)
	}
	if m.Status != MemberAlive {
		t.Errorf("status = %v, want alive", m.Status)
	}
}

func TestAddWithGossip_DoesNotInflateAliveIncarnation(t *testing.T) {
	// Re-registering a member already believed alive must not advance its
	// epoch. Only the node itself may do that; inflating it here would let a
	// peer's registration outrank the node's own later state, and the harness
	// re-meshes on a tight loop, so any inflation would compound.
	ml := NewMemberList("self", "self:80", nil)
	ml.Merge([]*Member{{ID: "n2", Address: "n2:80", Incarnation: 5, Status: MemberAlive}})

	for i := 0; i < 10; i++ {
		ml.AddWithGossip("n2", "n2:80", "")
	}

	m, _ := ml.Get("n2")
	if m.Incarnation != 5 {
		t.Errorf("incarnation = %d after 10 re-registrations, want 5 unchanged", m.Incarnation)
	}
}

func TestAddWithGossip_UnknownMemberStartsAtZero(t *testing.T) {
	// A member we have never seen carries no history to outrank, so it starts
	// at incarnation 0 and lets the node's own gossip take over from there.
	ml := NewMemberList("self", "self:80", nil)

	ml.AddWithGossip("n2", "n2:80", "")

	m, _ := ml.Get("n2")
	if m.Incarnation != 0 {
		t.Errorf("incarnation = %d for a first registration, want 0", m.Incarnation)
	}
}

// TestAddWithGossip_NodeOwnRefutationStillWins guards the ordering that makes
// the bump safe: the node itself advances its incarnation on refutation, so its
// own assertion must still be able to overtake a registration made about it.
func TestAddWithGossip_NodeOwnRefutationStillWins(t *testing.T) {
	ml := NewMemberList("self", "self:80", nil)
	ml.Merge([]*Member{{ID: "n2", Address: "n2:80", Incarnation: 3, Status: MemberDead}})

	ml.AddWithGossip("n2", "n2:80", "") // lands at incarnation 4

	// n2 refutes with a higher epoch and a new address; that must win.
	ml.Merge([]*Member{{ID: "n2", Address: "n2:9090", Incarnation: 5, Status: MemberAlive}})

	m, _ := ml.Get("n2")
	if m.Incarnation != 5 || m.Address != "n2:9090" {
		t.Errorf("member = inc %d addr %s, want inc 5 addr n2:9090", m.Incarnation, m.Address)
	}
}

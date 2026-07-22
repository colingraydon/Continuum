package gossip

import "testing"

func TestSetSelfZone_BumpsHeartbeat(t *testing.T) {
	// Arrange
	ml := NewMemberList("node1", "10.0.0.1:8080", nil)

	// Act
	ml.SetSelfZone("rack1")

	// Assert: zone set and heartbeat advanced so the change gossips out.
	self, ok := ml.Get("node1")
	if !ok {
		t.Fatal("self member missing")
	}
	if self.Zone != "rack1" {
		t.Errorf("expected zone rack1, got %q", self.Zone)
	}
	if self.Heartbeat == 0 {
		t.Error("expected heartbeat bump after zone change")
	}
}

func TestMerge_MetadataRefreshRefiresAlive(t *testing.T) {
	// Arrange: a member first registered without metadata (mesh stub via
	// POST /nodes) whose own gossip later arrives with zone and weight. The
	// status stays alive both times, but the ring only learns about members
	// through the callback, so the merge must re-fire it.
	var events []string
	receiver := NewMemberList("node2", "10.0.0.2:8080", func(m *Member, status MemberStatus) {
		if m.ID == "node1" {
			events = append(events, m.Zone)
		}
	})
	receiver.Add("node1", "10.0.0.1:8080")

	sender := NewMemberList("node1", "10.0.0.1:8080", nil)
	sender.SetSelfZone("rack1")
	sender.SetSelfWeight(2.0)

	// Act
	receiver.Merge(sender.GetAll())

	// Assert: first event from the stub add (no zone), second from the merge.
	if len(events) != 2 || events[1] != "rack1" {
		t.Fatalf("expected refresh callback with zone rack1, got events %q", events)
	}

	// A merge with no metadata change must stay quiet.
	sender.IncrementHeartbeat()
	receiver.Merge(sender.GetAll())
	if len(events) != 2 {
		t.Fatalf("expected no callback on heartbeat-only merge, got events %q", events)
	}
}

func TestSetSelfDC_BumpsHeartbeat(t *testing.T) {
	// Arrange
	ml := NewMemberList("node1", "10.0.0.1:8080", nil)

	// Act
	ml.SetSelfDC("us-east")

	// Assert: DC set and heartbeat advanced so the change gossips out.
	self, ok := ml.Get("node1")
	if !ok {
		t.Fatal("self member missing")
	}
	if self.DC != "us-east" {
		t.Errorf("expected dc us-east, got %q", self.DC)
	}
	if self.Heartbeat == 0 {
		t.Error("expected heartbeat bump after dc change")
	}
}

func TestMerge_DCChangeRefiresAlive(t *testing.T) {
	// Arrange: an already-known alive member whose only change is its DC. The
	// status stays alive, so the merge must still re-fire the callback for the
	// ring to relabel the node (covers the DC branch of notifyMemberChange).
	var events []string
	receiver := NewMemberList("node2", "10.0.0.2:8080", func(m *Member, status MemberStatus) {
		if m.ID == "node1" && status == MemberAlive {
			events = append(events, m.DC)
		}
	})

	sender := NewMemberList("node1", "10.0.0.1:8080", nil)
	receiver.Merge(sender.GetAll()) // first sighting, no DC
	sender.SetSelfDC("us-east")

	// Act
	receiver.Merge(sender.GetAll())

	// Assert: first event from the initial merge (no DC), second carries the DC.
	if len(events) != 2 || events[1] != "us-east" {
		t.Fatalf("expected refresh callback with dc us-east, got events %q", events)
	}
}

func TestMerge_PropagatesDC(t *testing.T) {
	// Arrange: node1 advertises a DC; node2 learns of it via merge and the
	// membership callback carries the DC for the ring to consume.
	sender := NewMemberList("node1", "10.0.0.1:8080", nil)
	sender.SetSelfDC("us-east")

	var callbackDC string
	receiver := NewMemberList("node2", "10.0.0.2:8080", func(m *Member, status MemberStatus) {
		if m.ID == "node1" && status == MemberAlive {
			callbackDC = m.DC
		}
	})

	// Act
	receiver.Merge(sender.GetAll())

	// Assert
	learned, ok := receiver.Get("node1")
	if !ok {
		t.Fatal("node1 not merged")
	}
	if learned.DC != "us-east" {
		t.Errorf("expected merged dc us-east, got %q", learned.DC)
	}
	if callbackDC != "us-east" {
		t.Errorf("expected onChange to carry dc us-east, got %q", callbackDC)
	}
}

func TestMerge_PropagatesZone(t *testing.T) {
	// Arrange: node1 advertises a zone; node2 learns of it via merge and the
	// membership callback carries the zone for the ring to consume.
	sender := NewMemberList("node1", "10.0.0.1:8080", nil)
	sender.SetSelfZone("rack1")

	var callbackZone string
	receiver := NewMemberList("node2", "10.0.0.2:8080", func(m *Member, status MemberStatus) {
		if m.ID == "node1" && status == MemberAlive {
			callbackZone = m.Zone
		}
	})

	// Act
	receiver.Merge(sender.GetAll())

	// Assert
	learned, ok := receiver.Get("node1")
	if !ok {
		t.Fatal("node1 not merged")
	}
	if learned.Zone != "rack1" {
		t.Errorf("expected merged zone rack1, got %q", learned.Zone)
	}
	if callbackZone != "rack1" {
		t.Errorf("expected onChange to carry zone rack1, got %q", callbackZone)
	}
}

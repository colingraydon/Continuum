package gossip

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// TestMemberListConcurrentReadersAndMutators pins the snapshot contract of
// Get/GetAll/GetAlive: readers must be able to inspect and marshal returned
// members while status transitions, heartbeats, and merges mutate the list.
// Before the getters returned copies, this raced (caught under -race by the
// simulation harness: the gossip round marshaled a member while the stale
// checker marked it dead).
func TestMemberListConcurrentReadersAndMutators(t *testing.T) {
	ml := NewMemberList("self", "self:80", nil)
	for i := 0; i < 8; i++ {
		ml.Add(fmt.Sprintf("n%d", i), fmt.Sprintf("n%d:80", i))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hammerReads(t, ml, stop)
		}()
	}

	for i := 0; i < 500; i++ {
		mutateOnce(ml, i)
	}
	close(stop)
	wg.Wait()
}

// hammerReads loops over every read accessor until stop closes, so the race
// detector sees reads concurrent with mutations.
func hammerReads(t *testing.T, ml *MemberList, stop <-chan struct{}) {
	t.Helper()
	for {
		select {
		case <-stop:
			return
		default:
		}
		if _, err := json.Marshal(ml.GetAll()); err != nil {
			t.Errorf("marshal: %v", err)
			return
		}
		for _, m := range ml.GetAlive() {
			_ = m.Status.String()
		}
		if m, ok := ml.Get("n3"); ok {
			_ = m.Heartbeat
		}
	}
}

// mutateOnce applies every in-place mutator to one member, cycling through
// the member set by iteration index.
func mutateOnce(ml *MemberList, i int) {
	id := fmt.Sprintf("n%d", i%8)
	ml.MarkSuspect(id)
	ml.MarkDead(id)
	ml.IncrementHeartbeat()
	ml.Merge([]*Member{{ID: id, Address: id + ":80", Incarnation: uint64(i), Heartbeat: uint64(i), Status: MemberAlive}})
	ml.SetBootstrapping(id, i%2 == 0)
}

// TestMemberListGetReturnsCopy pins that mutating a returned member does not
// reach the list's internal state.
func TestMemberListGetReturnsCopy(t *testing.T) {
	ml := NewMemberList("self", "self:80", nil)
	ml.Add("a", "a:80")

	m, ok := ml.Get("a")
	if !ok {
		t.Fatal("member a missing")
	}
	m.Status = MemberDead

	fresh, _ := ml.Get("a")
	if fresh.Status != MemberAlive {
		t.Fatal("mutation through a Get result must not reach the member list")
	}
}

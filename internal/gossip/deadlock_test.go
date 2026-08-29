package gossip

import (
	"sync"
	"testing"
	"time"
)

// TestMemberList_CallbacksFireOutsideLock is the regression for an AB-BA
// deadlock between the member list and the ring.
//
// The production wiring has two edges: the gossip onChange callback takes the
// ring lock (to add or remove the node), and the ring's health filter takes
// ml.mu (to read member status) from inside ring methods that already hold the
// ring lock. If MemberList invokes onChange while holding ml.mu, those two
// orders form a cycle: a Merge on the gossip receive path blocks on the ring
// lock while a request path holds the ring lock and blocks on ml.mu.
//
// This models it with a callback that reaches back into the member list, which
// is exactly what the ring's health filter does. Under the old code, where
// Merge and MarkDead called out under the lock, this hangs.
func TestMemberList_CallbacksFireOutsideLock(t *testing.T) {
	var ml *MemberList

	// The callback re-enters the member list, standing in for the ring's
	// health filter reading member status.
	reentered := make(chan struct{}, 16)
	ml = NewMemberList("self", "self:80", func(m *Member, _ MemberStatus) {
		ml.Get(m.ID)
		ml.GetAll()
		select {
		case reentered <- struct{}{}:
		default:
		}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		ml.Merge([]*Member{{ID: "n2", Address: "n2:80", Incarnation: 1, Status: MemberAlive}})
		ml.MarkSuspect("n2")
		ml.MarkDead("n2")
		ml.AddWithGossip("n2", "n2:80", "n2:81")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: a member-list mutator invoked its callback while holding ml.mu")
	}
	if len(reentered) == 0 {
		t.Error("callback never fired; the test is not exercising the path")
	}
}

// TestMemberList_ConcurrentMutatorsAndReentrantCallback drives both directions
// at once, the shape the live cluster hits: gossip merges on one side,
// status reads on the other, with a callback that re-enters on every change.
func TestMemberList_ConcurrentMutatorsAndReentrantCallback(t *testing.T) {
	var ml *MemberList
	ml = NewMemberList("self", "self:80", func(m *Member, _ MemberStatus) {
		ml.Get(m.ID)
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := uint64(1); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ml.Merge([]*Member{{ID: "n2", Address: "n2:80", Incarnation: i, Status: MemberAlive}})
				ml.MarkSuspect("n2")
				ml.MarkDead("n2")
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ml.GetAll()
				ml.GetAlive()
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock under concurrent mutators and a re-entrant callback")
	}
}

package antientropy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func TestStart_CancelContextStops(t *testing.T) {
	r, s, _ := newSyncNode(t, "node1")
	r.AddNode("node1", "127.0.0.1:0")

	mgr := New(r, s, "node1", 1, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)
	// Cancel immediately so syncLoop exits the ctx.Done branch.
	cancel()
	// Brief pause to let the goroutine observe cancellation.
	time.Sleep(10 * time.Millisecond)
}

func TestSyncRound_SkipsEmptyTrees(t *testing.T) {
	// A manager with no primary vnode ranges has no trees; syncRound is a no-op.
	r := ring.NewRing(50)
	r.AddNode("node2", "10.0.0.2") // only node2; node1 has no primary ranges
	s := store.New()

	mgr := New(r, s, "node1", 2, time.Second)
	mgr.syncRound() // must not panic
}

func TestSyncRound_SyncsToReplica(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")
	v := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}}
	s1.Put(key, "synced", v)

	mgr := New(r1, s1, "node1", 2, time.Second)

	// Drive all primary vnodes to guarantee the key's vnode is synced.
	syncAll(t, mgr)

	// Also exercise the syncRound code path (random vnode pick).
	for i := 0; i < 10; i++ {
		mgr.syncRound()
	}

	entry, ok := s2.Get(key)
	if !ok {
		t.Fatal("replica did not receive key after syncRound")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "synced" {
		t.Errorf("unexpected replica entry: %+v", entry.Siblings)
	}
}

func TestRunGC_FreshTombstoneNotPurged(t *testing.T) {
	r, s, _ := newSyncNode(t, "node1")
	r.AddNode("node1", "127.0.0.1:0")

	key := firstPrimaryKey(r, "node1")
	s.Delete(key, store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	mgr := New(r, s, "node1", 1, time.Second)
	mgr.runGC() // tombstone is fresh; gcTTL is 1h so nothing is purged

	if _, ok := s.Get(key); !ok {
		t.Error("fresh tombstone should not be purged by runGC")
	}
}

func TestRunGC_NothingToGC(t *testing.T) {
	r, s, _ := newSyncNode(t, "node1")
	r.AddNode("node1", "127.0.0.1:0")

	mgr := New(r, s, "node1", 1, time.Second)
	mgr.runGC() // empty store, should not panic or log
}

func TestPushSyncEntries_NonOK(t *testing.T) {
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	r := ring.NewRing(50)
	s := store.New()
	mgr := New(r, s, "node1", 1, time.Second)

	addr := strings.TrimPrefix(badSrv.URL, "http://")
	err := mgr.pushSyncEntries(addr, map[string][]syncSibling{
		"k": {{Value: "v", Clocks: map[string]uint64{"n1": 1}}},
	})
	if err == nil {
		t.Error("expected error when server returns non-204")
	}
}

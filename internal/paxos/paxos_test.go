package paxos

import (
	"fmt"
	"testing"
)

func b(n uint64, node string) Ballot { return Ballot{Counter: n, Node: node} }

func TestBallotOrdering(t *testing.T) {
	cases := []struct {
		a, b Ballot
		less bool
	}{
		{b(1, "n1"), b(2, "n1"), true},
		{b(2, "n1"), b(1, "n1"), false},
		{b(1, "n1"), b(1, "n2"), true},
		{b(1, "n1"), b(1, "n1"), false},
	}
	for _, c := range cases {
		if got := c.a.Less(c.b); got != c.less {
			t.Errorf("%v.Less(%v) = %v, want %v", c.a, c.b, got, c.less)
		}
	}
}

func TestBallotStringAndZero(t *testing.T) {
	if got := b(7, "n1").String(); got != "7@n1" {
		t.Errorf("String() = %q, want 7@n1", got)
	}
	if !(Ballot{}).IsZero() || b(1, "n1").IsZero() {
		t.Error("IsZero must hold only for the zero ballot")
	}
}

func TestPrepareAcceptCommitRound(t *testing.T) {
	a := NewAcceptor()

	p, err := a.Prepare("k", b(10, "n1"))
	if err != nil || !p.OK || p.Accepted != nil {
		t.Fatalf("fresh prepare: %+v err=%v", p, err)
	}

	m := Mutation{Key: "k", Value: "v1", Clocks: map[string]uint64{"n1": 1}, Ballot: b(10, "n1")}
	if p, err = a.Accept(m); err != nil || !p.OK {
		t.Fatalf("accept: %+v err=%v", p, err)
	}

	// A later prepare must learn the accepted-uncommitted mutation.
	p, err = a.Prepare("k", b(11, "n2"))
	if err != nil || !p.OK || p.Accepted == nil || p.Accepted.Value != "v1" {
		t.Fatalf("prepare after accept: %+v err=%v", p, err)
	}

	if err := a.Commit("k", b(10, "n1")); err != nil {
		t.Fatalf("commit: %v", err)
	}
	p, err = a.Prepare("k", b(12, "n1"))
	if err != nil || !p.OK || p.Accepted != nil {
		t.Fatalf("prepare after commit must be clean: %+v err=%v", p, err)
	}
}

func TestPrepareRejectsLowerBallot(t *testing.T) {
	a := NewAcceptor()
	if _, err := a.Prepare("k", b(10, "n2")); err != nil {
		t.Fatal(err)
	}
	p, err := a.Prepare("k", b(9, "n1"))
	if err != nil || p.OK {
		t.Fatalf("lower prepare must be rejected: %+v err=%v", p, err)
	}
	if p.Promised != b(10, "n2") {
		t.Fatalf("rejection must report the winning ballot, got %v", p.Promised)
	}
}

func TestAcceptRejectsSupersededBallot(t *testing.T) {
	a := NewAcceptor()
	if _, err := a.Prepare("k", b(10, "n1")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prepare("k", b(20, "n2")); err != nil {
		t.Fatal(err)
	}
	p, err := a.Accept(Mutation{Key: "k", Value: "v", Ballot: b(10, "n1")})
	if err != nil || p.OK {
		t.Fatalf("accept below promise must be rejected: %+v err=%v", p, err)
	}
}

func TestCommitRecordsBallotAndDropsOlderDebris(t *testing.T) {
	a := NewAcceptor()
	if _, err := a.Prepare("k", b(5, "n1")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Accept(Mutation{Key: "k", Value: "old", Ballot: b(5, "n1")}); err != nil {
		t.Fatal(err)
	}
	// A commit for a later round clears sub-committed debris and records the
	// committed ballot for coordinators to filter against.
	if err := a.Commit("k", b(9, "n2")); err != nil {
		t.Fatal(err)
	}
	p, err := a.Prepare("k", b(12, "n1"))
	if err != nil || !p.OK {
		t.Fatalf("prepare: %+v err=%v", p, err)
	}
	if p.Accepted != nil {
		t.Errorf("debris at ballot 5 must be cleared by a commit at 9, got %+v", p.Accepted)
	}
	if p.Committed != b(9, "n2") {
		t.Errorf("promise must report committed ballot 9, got %v", p.Committed)
	}
}

func TestCommittedBallotSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Commit("k", b(9, "n2")); err != nil {
		t.Fatal(err)
	}
	a2, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	p, err := a2.Prepare("k", b(12, "n1"))
	if err != nil || p.Committed != b(9, "n2") {
		t.Fatalf("committed ballot lost across restart: %+v err=%v", p, err)
	}
}

func TestCommitOnlyClearsItsOwnRound(t *testing.T) {
	a := NewAcceptor()
	if _, err := a.Accept(Mutation{Key: "k", Value: "v2", Ballot: b(20, "n2")}); err != nil {
		t.Fatal(err)
	}
	// A stale commit from a slower round must not clear the newer accept.
	if err := a.Commit("k", b(10, "n1")); err != nil {
		t.Fatal(err)
	}
	p, err := a.Prepare("k", b(30, "n1"))
	if err != nil || p.Accepted == nil || p.Accepted.Value != "v2" {
		t.Fatalf("newer accepted state lost to a stale commit: %+v err=%v", p, err)
	}
}

func TestPromisesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prepare("k", b(10, "n1")); err != nil {
		t.Fatal(err)
	}
	m := Mutation{Key: "k", Value: "v1", Clocks: map[string]uint64{"n1": 1}, Ballot: b(10, "n1")}
	if _, err := a.Accept(m); err != nil {
		t.Fatal(err)
	}
	// No clean close: simulate a crash by reopening the directory.
	a2, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()

	// The pre-crash promise must still gate lower ballots...
	p, err := a2.Prepare("k", b(9, "n2"))
	if err != nil || p.OK {
		t.Fatalf("promise forgotten across restart: %+v err=%v", p, err)
	}
	// ...and the accepted mutation must still be reported for resurrection.
	p, err = a2.Prepare("k", b(11, "n2"))
	if err != nil || !p.OK || p.Accepted == nil || p.Accepted.Value != "v1" {
		t.Fatalf("accepted mutation lost across restart: %+v err=%v", p, err)
	}
}

func TestRecoveryCompactsLog(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Many rounds on few keys: the log holds one record per state change.
	for i := uint64(1); i <= 50; i++ {
		key := fmt.Sprintf("k%d", i%3)
		bal := b(i, "n1")
		if _, err := a.Prepare(key, bal); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Accept(Mutation{Key: key, Value: "v", Ballot: bal}); err != nil {
			t.Fatal(err)
		}
		if err := a.Commit(key, bal); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	a2, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := a2.Close(); err != nil {
		t.Fatal(err)
	}

	// After compaction a third open replays at most one record per key.
	r, err := OpenAcceptor(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.mu.Lock()
	keys := len(r.keys)
	r.mu.Unlock()
	if keys != 3 {
		t.Fatalf("expected 3 keys after recovery, got %d", keys)
	}
	// State must have survived both compactions.
	p, err := r.Prepare("k1", b(1, "n0"))
	if err != nil || p.OK {
		t.Fatalf("compaction lost the promise gate: %+v err=%v", p, err)
	}
}

func TestMemoryAcceptorCloseIsNoop(t *testing.T) {
	a := NewAcceptor()
	if _, err := a.Prepare("k", b(1, "n1")); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

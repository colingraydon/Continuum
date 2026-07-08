package histcheck

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

const checkTimeout = 10 * time.Second

// seqOps builds a history where op i occupies the interval [10i, 10i+5]:
// strictly sequential, no concurrency.
func seqOps(ops []Op) []Op {
	for i := range ops {
		ops[i].Call = int64(i * 10)
		ops[i].Return = int64(i*10 + 5)
	}
	return ops
}

func TestSequentialCASHistoryLinearizes(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: Read, Found: false},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 1, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusOK},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "v1", Value: "v3", Status: StatusConflict},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v2"},
	})
	if r := Check(ops, checkTimeout); !r.Linearizable() {
		t.Fatalf("sequential CAS history must linearize (result undecided=%v)", r.Undecided())
	}
}

func TestLostUpdateIsFlagged(t *testing.T) {
	// Two clients read v1 and both CAS from it successfully: a fork. No
	// sequential CAS register permits both to succeed.
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusOK},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "v1", Value: "v3", Status: StatusOK},
	})
	if r := Check(ops, checkTimeout); r.Linearizable() {
		t.Fatal("forked CAS history must not linearize")
	}
}

func TestStaleReadIsFlagged(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusOK},
		{Client: 1, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	if r := Check(ops, checkTimeout); r.Linearizable() {
		t.Fatal("read of an overwritten value after the overwrite returned must not linearize")
	}
}

func TestSpuriousConflictIsFlagged(t *testing.T) {
	// A 412 whose precondition actually matched the current state.
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusConflict},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	if r := Check(ops, checkTimeout); r.Linearizable() {
		t.Fatal("spurious 412 must not linearize")
	}
}

func TestUnknownOutcomeMayApply(t *testing.T) {
	// The unacknowledged write is later observed: it must be allowed to
	// linearize at some point before the read.
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusUnknown},
		{Client: 1, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	if r := Check(ops, checkTimeout); !r.Linearizable() {
		t.Fatal("unknown-outcome write observed by a later read must linearize")
	}
}

func TestUnknownOutcomeMayNeverApply(t *testing.T) {
	// The unacknowledged write is never observed: equally fine.
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusUnknown},
		{Client: 1, Key: "k", Kind: Read, Found: false},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "", Value: "v2", Status: StatusOK},
		{Client: 1, Key: "k", Kind: Read, Found: true, ReadValue: "v2"},
	})
	if r := Check(ops, checkTimeout); !r.Linearizable() {
		t.Fatal("unobserved unknown-outcome write must linearize as never-applied")
	}
}

func TestConflictReadIsFlagged(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 1, Key: "k", Kind: Read, Found: true, Conflict: true},
	})
	if r := Check(ops, checkTimeout); r.Linearizable() {
		t.Fatal("a sibling conflict in a pure-CAS history must not linearize")
	}
}

func TestKeysCheckIndependently(t *testing.T) {
	// Interleaved keys with per-key-consistent histories linearize even
	// though a single shared register could not serve both.
	ops := seqOps([]Op{
		{Client: 0, Key: "a", Kind: CASPut, Expected: "", Value: "a1", Status: StatusOK},
		{Client: 1, Key: "b", Kind: CASPut, Expected: "", Value: "b1", Status: StatusOK},
		{Client: 0, Key: "a", Kind: Read, Found: true, ReadValue: "a1"},
		{Client: 1, Key: "b", Kind: Read, Found: true, ReadValue: "b1"},
	})
	if r := Check(ops, checkTimeout); !r.Linearizable() {
		t.Fatal("independent per-key histories must linearize")
	}
}

func TestConcurrentCASOnlyOneWins(t *testing.T) {
	// Fully overlapping CAS ops from the same expected value: exactly one
	// 204 and one 412 is the only linearizable outcome, in either order.
	ops := []Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK, Call: 0, Return: 5},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "v1", Value: "v2", Status: StatusOK, Call: 10, Return: 30},
		{Client: 2, Key: "k", Kind: CASPut, Expected: "v1", Value: "v3", Status: StatusConflict, Call: 10, Return: 30},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v2", Call: 40, Return: 45},
	}
	if r := Check(ops, checkTimeout); !r.Linearizable() {
		t.Fatal("one winner and one 412 among concurrent CAS must linearize")
	}
}

func TestVisualizeWritesFile(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	r := Check(ops, checkTimeout)
	path := filepath.Join(t.TempDir(), "history.html")
	if err := r.Visualize(path); err != nil {
		t.Fatalf("visualize: %v", err)
	}
	if st, err := os.Stat(path); err != nil || st.Size() == 0 {
		t.Fatalf("visualization file missing or empty: %v", err)
	}
}

// TestVisualizeFailingHistory renders the case visualization exists for: a
// violating history, exercising every operation description — absent reads,
// conflict reads, unknown-outcome and rejected writes — and the absent state.
func TestVisualizeFailingHistory(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: Read, Found: false},
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusUnknown},
		{Client: 1, Key: "k", Kind: CASPut, Expected: "", Value: "v2", Status: StatusConflict},
		{Client: 1, Key: "k", Kind: Read, Found: true, Conflict: true},
	})
	r := Check(ops, checkTimeout)
	if r.Linearizable() {
		t.Fatal("conflict-read history must not linearize")
	}
	path := filepath.Join(t.TempDir(), "failing.html")
	if err := r.Visualize(path); err != nil {
		t.Fatalf("visualize: %v", err)
	}
	if st, err := os.Stat(path); err != nil || st.Size() == 0 {
		t.Fatalf("visualization file missing or empty: %v", err)
	}
}

func TestResultAccessors(t *testing.T) {
	ops := seqOps([]Op{
		{Client: 0, Key: "k", Kind: CASPut, Expected: "", Value: "v1", Status: StatusOK},
		{Client: 0, Key: "k", Kind: Read, Found: true, ReadValue: "v1"},
	})
	r := Check(ops, checkTimeout)
	if r.Ops() != len(ops) {
		t.Errorf("Ops() = %d, want %d", r.Ops(), len(ops))
	}
	if r.Undecided() {
		t.Error("a decided check must not report Undecided")
	}
	// An exceeded search timeout is the one path that yields Undecided;
	// porcupine reports it as the Unknown check result.
	timedOut := Result{result: porcupine.Unknown, ops: 3}
	if !timedOut.Undecided() || timedOut.Linearizable() {
		t.Errorf("porcupine.Unknown must map to Undecided, got undecided=%v linearizable=%v",
			timedOut.Undecided(), timedOut.Linearizable())
	}
}

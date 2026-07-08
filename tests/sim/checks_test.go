//go:build sim

package sim

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/histcheck"
	"github.com/colingraydon/continuum/internal/store"
)

// verifyDurability asserts that after faults heal, every key's last
// acknowledged write is visible through some coordinator at or beyond its
// acknowledged sequence.
func verifyDurability(t *testing.T, c *simCluster, w *rmwWorkload, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	pending := make(map[string]*keyState)
	for _, k := range w.keys {
		if k.lastAcked > 0 {
			pending[k.key] = k
		}
	}
	for len(pending) > 0 && time.Now().Before(deadline) {
		for key, k := range pending {
			if durableOnce(c, k) {
				delete(pending, key)
			}
		}
		if len(pending) > 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	for key, k := range pending {
		t.Errorf("durability violated for %s: last acked seq %d not visible", key, k.lastAcked)
	}
}

func durableOnce(c *simCluster, k *keyState) bool {
	for _, n := range c.running() {
		nr, code, err := c.get(n, k.key)
		if err != nil || code != http.StatusOK {
			continue
		}
		max := seqOf(nr.Value)
		for _, s := range nr.Siblings {
			if v := seqOf(s.Value); v > max {
				max = v
			}
		}
		if max >= k.lastAcked {
			return true
		}
	}
	return false
}

// verifyConvergence asserts that every running replica of each key holds an
// identical, non-empty sibling set — checked by direct store access, which an
// in-process cluster gets for free.
func verifyConvergence(t *testing.T, c *simCluster, keys []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	pending := make(map[string]bool, len(keys))
	for _, k := range keys {
		pending[k] = true
	}
	for len(pending) > 0 && time.Now().Before(deadline) {
		for key := range pending {
			if ok, _ := convergedOnce(c, key); ok {
				delete(pending, key)
			}
		}
		if len(pending) > 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	for key := range pending {
		_, detail := convergedOnce(c, key)
		t.Errorf("replicas did not converge for %s: %s", key, detail)
	}
}

func convergedOnce(c *simCluster, key string) (bool, string) {
	replicas := c.replicaSet(key)
	if len(replicas) == 0 {
		return false, "no replica set resolvable"
	}
	states := make(map[string][]string)
	for _, rn := range replicas {
		if !rn.running.Load() {
			continue
		}
		entry, ok, err := rn.store.Get(key)
		switch {
		case err != nil:
			states[fmt.Sprintf("error(%v)", err)] = append(states[fmt.Sprintf("error(%v)", err)], rn.id)
		case !ok:
			states["absent"] = append(states["absent"], rn.id)
		default:
			s := canonicalEntry(entry)
			states[s] = append(states[s], rn.id)
		}
	}
	if len(states) != 1 {
		var parts []string
		for state, ids := range states {
			parts = append(parts, fmt.Sprintf("%v=%s", ids, state))
		}
		sort.Strings(parts)
		return false, strings.Join(parts, " vs ")
	}
	for s := range states {
		if s == "absent" {
			return false, "all replicas empty"
		}
	}
	return true, ""
}

// canonicalEntry reduces a store entry to a comparable string: each sibling
// as value|deleted|sorted-clocks, siblings sorted.
func canonicalEntry(e store.Entry) string {
	parts := make([]string, 0, len(e.Siblings))
	for _, sib := range e.Siblings {
		ck := make([]string, 0, len(sib.Version.Clocks))
		for id, v := range sib.Version.Clocks {
			ck = append(ck, fmt.Sprintf("%s=%d", id, v))
		}
		sort.Strings(ck)
		parts = append(parts, fmt.Sprintf("%s|%t|%s", sib.Value, sib.Deleted, strings.Join(ck, ",")))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// verifyLinearizable runs the porcupine check as a hard assertion: the
// history must be proven linearizable within the search timeout.
func verifyLinearizable(t *testing.T, w *casWorkload, timeout time.Duration) {
	t.Helper()
	switch checkLinearizable(t, w, timeout) {
	case histcheck.VerdictNotLinearizable:
		t.Errorf("history NOT linearizable: a CAS write forked or was lost")
	case histcheck.VerdictUndecided:
		t.Errorf("linearizability undecided after %v; shrink the workload or raise the timeout", timeout)
	}
}

// expectKnownCASGap runs the check in detector mode: under membership churn
// the primary-serialized CAS design is known not to linearize (fault harness
// finding #7); report, don't fail, until consensus-backed CAS closes it.
func expectKnownCASGap(t *testing.T, w *casWorkload, timeout time.Duration) {
	t.Helper()
	switch checkLinearizable(t, w, timeout) {
	case histcheck.VerdictLinearizable:
		t.Log("no CAS violation surfaced this run (churn window is timing-dependent)")
	case histcheck.VerdictNotLinearizable:
		t.Log("known CAS gap reproduced under simulated churn (finding #7, docs/fault-injection.md)")
	case histcheck.VerdictUndecided:
		t.Logf("linearizability undecided after %v (fine in detector mode)", timeout)
	}
}

// checkLinearizable runs the shared check, logging the outcome (and the
// visualization path on a violation) and returning the verdict for the
// caller to act on.
func checkLinearizable(t *testing.T, w *casWorkload, timeout time.Duration) histcheck.Verdict {
	t.Helper()
	verdict, ops, path := histcheck.CheckAndVisualize(w.rec.History(), timeout, t.Name())
	switch verdict {
	case histcheck.VerdictLinearizable:
		t.Logf("history linearizable: %d ops", ops)
	case histcheck.VerdictNotLinearizable:
		if path != "" {
			t.Logf("linearization visualization: %s (%d ops)", path, ops)
		}
	}
	return verdict
}

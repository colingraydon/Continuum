//go:build fault

package fault

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

// verifyDurability asserts the quorum durability invariant: for every key,
// the last acknowledged (204) write must still be visible through a
// consistent read once faults heal. A returned value is acceptable if its
// sequence is >= the last acked one: a later, unacknowledged (timed-out or
// 503) write may legitimately have won, but nothing older than the last ack
// may survive as the sole result.
func verifyDurability(t *testing.T, c *cluster, w *workload, timeout time.Duration) {
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
			time.Sleep(500 * time.Millisecond)
		}
	}
	for key, k := range pending {
		nr, code, err := c.get(c.alive()[0], key)
		t.Errorf("durability violated for %s: last acked seq %d not visible (last read: code=%d err=%v resp=%+v)",
			key, k.lastAcked, code, err, nr)
	}
}

// durableOnce returns true if some alive coordinator serves key with a value
// at or beyond the last acknowledged sequence.
func durableOnce(c *cluster, k *keyState) bool {
	for _, n := range c.alive() {
		nr, code, err := c.get(n, k.key)
		if err != nil || code != http.StatusOK {
			continue
		}
		if maxSeqIn(nr) >= k.lastAcked {
			return true
		}
	}
	return false
}

func maxSeqIn(nr nodeResponse) uint64 {
	max := seqOf(nr.Value)
	for _, s := range nr.Siblings {
		if v := seqOf(s.Value); v > max {
			max = v
		}
	}
	return max
}

// verifyConvergence asserts eventual consistency: after faults heal, every
// running replica of each key holds an identical, non-empty sibling set.
func verifyConvergence(t *testing.T, c *cluster, keys []string, timeout time.Duration) {
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
			time.Sleep(500 * time.Millisecond)
		}
	}
	for key := range pending {
		_, detail := convergedOnce(c, key)
		t.Errorf("replicas did not converge for %s: %s", key, detail)
	}
}

// convergedOnce checks whether all running replicas of key hold the same
// non-empty state right now. detail describes the divergence for reporting.
func convergedOnce(c *cluster, key string) (bool, string) {
	replicas := c.replicaSet(key)
	if len(replicas) == 0 {
		return false, "no replica set resolvable"
	}
	states := make(map[string][]string)
	for _, rn := range replicas {
		if !rn.isRunning() {
			continue
		}
		nr, code, err := c.directGet(rn, key)
		if err != nil || code != http.StatusOK {
			k := fmt.Sprintf("unreadable(code=%d,err=%v)", code, err)
			states[k] = append(states[k], rn.id)
			continue
		}
		k := canonicalState(nr)
		states[k] = append(states[k], rn.id)
	}
	if len(states) != 1 {
		return false, describeStates(states)
	}
	for s := range states {
		if s == "absent" {
			return false, "all replicas empty"
		}
	}
	return true, ""
}

// canonicalState reduces a direct read to a comparable string: each sibling
// as value|deleted|sorted-clocks, siblings sorted.
func canonicalState(nr nodeResponse) string {
	type sib struct {
		value   string
		deleted bool
		clocks  map[string]uint64
	}
	var sibs []sib
	if len(nr.Siblings) > 0 {
		for _, s := range nr.Siblings {
			sibs = append(sibs, sib{s.Value, s.Deleted, s.Clocks})
		}
	} else if nr.Value != "" || nr.Deleted || len(nr.Clocks) > 0 {
		sibs = append(sibs, sib{nr.Value, nr.Deleted, nr.Clocks})
	}
	if len(sibs) == 0 {
		return "absent"
	}
	parts := make([]string, len(sibs))
	for i, s := range sibs {
		ck := make([]string, 0, len(s.clocks))
		for id, v := range s.clocks {
			ck = append(ck, fmt.Sprintf("%s=%d", id, v))
		}
		sort.Strings(ck)
		parts[i] = fmt.Sprintf("%s|%t|%s", s.value, s.deleted, strings.Join(ck, ","))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

func describeStates(states map[string][]string) string {
	var parts []string
	for state, ids := range states {
		parts = append(parts, fmt.Sprintf("%v=%s", ids, state))
	}
	sort.Strings(parts)
	return strings.Join(parts, " vs ")
}

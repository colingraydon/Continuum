package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPutKeyClockBootstrapping(t *testing.T) {
	// Without bootstrapping, a second blind PUT increments from an empty clock
	// and produces {self:1} again, which equals the first write's clock and is
	// silently dropped as idempotent. Bootstrapping reads the current entry
	// first so the overwrite gets {self:2}, which dominates {self:1} and wins.
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	put := func(value string) int {
		req := httptest.NewRequest(http.MethodPut, "/keys/k", bytes.NewBufferString(`{"value":"`+value+`"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.PutKey(w, req)
		return w.Code
	}

	if code := put("v1"); code != http.StatusNoContent {
		t.Fatalf("first put: expected 204, got %d", code)
	}
	entry, ok, _ := h.store.Get("k")
	if !ok || entry.Siblings[0].Version.Clocks["self"] != 1 {
		t.Fatalf("expected value at clock {self:1}, got %+v", entry)
	}

	if code := put("v2"); code != http.StatusNoContent {
		t.Fatalf("second put: expected 204, got %d", code)
	}
	entry, ok, _ = h.store.Get("k")
	if !ok {
		t.Fatal("expected entry to exist after overwrite")
	}
	if len(entry.Siblings) != 1 {
		t.Fatalf("expected single sibling after overwrite, got %+v", entry.Siblings)
	}
	if entry.Siblings[0].Value != "v2" {
		t.Errorf("expected overwrite to win, got value %q", entry.Siblings[0].Value)
	}
	if entry.Siblings[0].Version.Clocks["self"] != 2 {
		t.Errorf("expected clock {self:2}, got %v", entry.Siblings[0].Version.Clocks)
	}
}

// newQuorumOneHandler returns a handler with WriteQuorum=1 and a fake replica
// that signals on the returned channel when it receives a request with the
// given method.
func newQuorumOneHandler(t *testing.T, method string) (*Handler, *httptest.Server, <-chan string) {
	t.Helper()
	received := make(chan string, 1)
	replica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == method {
			select {
			case received <- r.URL.Path:
			default:
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(replica.Close)

	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.memberList.Add("replica1", strings.TrimPrefix(replica.URL, "http://"))
	return h, replica, received
}

func TestPutKey_QuorumOneStillFansOut(t *testing.T) {
	// With W=1 the coordinator's own ack satisfies quorum, but the write must
	// still be replicated to the other replicas asynchronously.
	h, _, received := newQuorumOneHandler(t, http.MethodPut)

	req := httptest.NewRequest(http.MethodPut, "/keys/fanout", bytes.NewBufferString(`{"value":"v"}`))
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}

	select {
	case path := <-received:
		if path != "/keys/fanout" {
			t.Errorf("replica received unexpected path %q", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica never received the W=1 write")
	}
}

func TestDeleteKey_QuorumOneStillFansOut(t *testing.T) {
	h, _, received := newQuorumOneHandler(t, http.MethodDelete)

	req := httptest.NewRequest(http.MethodDelete, "/keys/fanout", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}

	select {
	case path := <-received:
		if path != "/keys/fanout" {
			t.Errorf("replica received unexpected path %q", path)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replica never received the W=1 delete")
	}
}

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func doScan(t *testing.T, h *Handler, query string, local bool) (*httptest.ResponseRecorder, ScanKeysResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/keys?"+query, nil)
	if local {
		req.Header.Set(headerXProxiedFrom, "test-peer")
	}
	w := httptest.NewRecorder()
	h.ScanKeys(w, req)
	var resp ScanKeysResponse
	if w.Code == http.StatusOK && !local {
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
	return w, resp
}

func TestScanKeysParamValidation(t *testing.T) {
	h := newTestHandler(t)
	cases := []struct {
		name, query string
	}{
		{"missing prefix", "limit=10"},
		{"garbage limit", "prefix=a&limit=nope"},
		{"zero limit", "prefix=a&limit=0"},
		{"negative limit", "prefix=a&limit=-5"},
	}
	for _, tc := range cases {
		if w, _ := doScan(t, h, tc.query, false); w.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, w.Code)
		}
	}
}

func TestScanKeysLocalMode(t *testing.T) {
	h := newTestHandler(t)
	seed := map[string]store.VectorClockVersion{
		"ls-a": {Clocks: map[string]uint64{"n1": 1}},
		"ls-b": {Clocks: map[string]uint64{"n1": 2}},
	}
	for k, v := range seed {
		if err := h.store.Put(k, "v-"+k, v); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := h.store.Delete("ls-b", store.VectorClockVersion{Clocks: map[string]uint64{"n1": 3}}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/keys?prefix=ls-", nil)
	req.Header.Set(headerXProxiedFrom, "test-peer")
	w := httptest.NewRecorder()
	h.ScanKeys(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("local scan: got %d: %s", w.Code, w.Body.String())
	}
	var resp ScanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Both keys present, ls-b as a tombstone (local mode keeps tombstones so
	// the coordinator can merge dominance).
	if len(resp.Items) != 2 || resp.Items[0].Key != "ls-a" || resp.Items[1].Key != "ls-b" {
		t.Fatalf("local scan items = %+v, want ls-a and ls-b", resp.Items)
	}
	if sibs := resp.Items[1].Siblings; len(sibs) != 1 || !sibs[0].Deleted {
		t.Errorf("ls-b siblings = %+v, want a lone tombstone", resp.Items[1].Siblings)
	}
}

// fakeScanNode serves the local-mode scan wire format with canned items.
func fakeScanNode(t *testing.T, items []ScanItem) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/keys" || req.Header.Get(headerXProxiedFrom) == "" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(ScanResponse{Items: items})
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func newScanHandler(t *testing.T) (*Handler, *gossip.MemberList) {
	t.Helper()
	r := ring.NewRing(10)
	ml := gossip.NewMemberList("self", "localhost", nil)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
	return h, ml
}

func clk(n string, c uint64) map[string]uint64 { return map[string]uint64{n: c} }

func TestScanKeysCoordinatorMerge(t *testing.T) {
	h, ml := newScanHandler(t)

	// Node A: stale value for m-1, live value for m-2, live m-3.
	ml.Add("nodeA", fakeScanNode(t, []ScanItem{
		{Key: "m-1", Siblings: []SiblingResponse{{Value: "old", Clocks: clk("w", 1)}}},
		{Key: "m-2", Siblings: []SiblingResponse{{Value: "left", Clocks: clk("a", 1)}}},
		{Key: "m-3", Siblings: []SiblingResponse{{Value: "live", Clocks: clk("w", 1)}}},
	}))
	// Node B: newer value for m-1 (dominates), concurrent value for m-2
	// (sibling), tombstone dominating m-3 (drops it).
	ml.Add("nodeB", fakeScanNode(t, []ScanItem{
		{Key: "m-1", Siblings: []SiblingResponse{{Value: "new", Clocks: clk("w", 2)}}},
		{Key: "m-2", Siblings: []SiblingResponse{{Value: "right", Clocks: clk("b", 1)}}},
		{Key: "m-3", Siblings: []SiblingResponse{{Deleted: true, Clocks: clk("w", 2)}}},
	}))

	w, resp := doScan(t, h, "prefix=m-&limit=100", false)
	if w.Code != http.StatusOK {
		t.Fatalf("scan: got %d: %s", w.Code, w.Body.String())
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items = %+v, want m-1 and m-2 only (m-3 fully deleted)", resp.Items)
	}
	if resp.Items[0].Key != "m-1" || resp.Items[0].Value != "new" {
		t.Errorf("m-1 = %+v, want dominated merge to yield %q", resp.Items[0], "new")
	}
	if resp.Items[1].Key != "m-2" || len(resp.Items[1].Siblings) != 2 {
		t.Errorf("m-2 = %+v, want 2 concurrent siblings", resp.Items[1])
	}
	if resp.Next != "" {
		t.Errorf("Next = %q, want empty (all nodes exhausted)", resp.Next)
	}
}

func TestScanKeysNodeFailureFailsClosed(t *testing.T) {
	h, ml := newScanHandler(t)
	deadSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadAddr := deadSrv.Listener.Addr().String()
	deadSrv.Close()
	ml.Add("deadNode", deadAddr)

	if w, _ := doScan(t, h, "prefix=m-&limit=10", false); w.Code != http.StatusServiceUnavailable {
		t.Errorf("scan with unreachable node: got %d, want 503", w.Code)
	}
}

// TestScanKeysHorizon: a node whose page filled caps the trusted range. Keys
// beyond the smallest full-page last-key are deferred, and Next resumes there.
func TestScanKeysHorizon(t *testing.T) {
	h, ml := newScanHandler(t)

	// limit=2. Node A fills its page (last key "h-2" = horizon). Node B is
	// exhausted after one item, but its "h-9" lies beyond the horizon: node A
	// may hold keys between h-2 and h-9 that its page cut off.
	ml.Add("nodeA", fakeScanNode(t, []ScanItem{
		{Key: "h-1", Siblings: []SiblingResponse{{Value: "a1", Clocks: clk("w", 1)}}},
		{Key: "h-2", Siblings: []SiblingResponse{{Value: "a2", Clocks: clk("w", 1)}}},
	}))
	ml.Add("nodeB", fakeScanNode(t, []ScanItem{
		{Key: "h-9", Siblings: []SiblingResponse{{Value: "b9", Clocks: clk("w", 1)}}},
	}))

	w, resp := doScan(t, h, "prefix=h-&limit=2", false)
	if w.Code != http.StatusOK {
		t.Fatalf("scan: got %d: %s", w.Code, w.Body.String())
	}
	got := make([]string, len(resp.Items))
	for i, it := range resp.Items {
		got[i] = it.Key
	}
	if len(got) != 2 || got[0] != "h-1" || got[1] != "h-2" {
		t.Fatalf("items = %v, want [h-1 h-2] (h-9 beyond horizon)", got)
	}
	if resp.Next != "h-2" {
		t.Errorf("Next = %q, want h-2 (resume at the horizon)", resp.Next)
	}
}

// TestScanKeysPeerErrorResponses: a peer answering non-200 or malformed JSON
// fails the scan closed, same as an unreachable peer.
func TestScanKeysPeerErrorResponses(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"peer 500", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}},
		{"peer garbage body", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("not json"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, ml := newScanHandler(t)
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			ml.Add("badPeer", srv.Listener.Addr().String())

			if w, _ := doScan(t, h, "prefix=m-&limit=10", false); w.Code != http.StatusServiceUnavailable {
				t.Errorf("got %d, want 503", w.Code)
			}
		})
	}
}

func TestScanKeysCoordinatorBootstrapping(t *testing.T) {
	h, ml := newScanHandler(t)
	ml.SetBootstrapping("self", true)
	if w, _ := doScan(t, h, "prefix=m-&limit=10", false); w.Code != http.StatusServiceUnavailable {
		t.Errorf("bootstrapping coordinator: got %d, want 503", w.Code)
	}
}

// TestScanKeysPageFullCut: more live in-horizon keys than limit cuts the page
// and resumes at the last emitted key.
func TestScanKeysPageFullCut(t *testing.T) {
	h, ml := newScanHandler(t)
	ml.Add("nodeA", fakeScanNode(t, []ScanItem{
		{Key: "pf-1", Siblings: []SiblingResponse{{Value: "a", Clocks: clk("w", 1)}}},
		{Key: "pf-2", Siblings: []SiblingResponse{{Value: "b", Clocks: clk("w", 1)}}},
		{Key: "pf-3", Siblings: []SiblingResponse{{Value: "c", Clocks: clk("w", 1)}}},
	}))

	// nodeA is exhausted (3 < limit 100? no - use limit 2: page cut at 2).
	w, resp := doScan(t, h, "prefix=pf-&limit=2", false)
	if w.Code != http.StatusOK {
		t.Fatalf("scan: got %d: %s", w.Code, w.Body.String())
	}
	if len(resp.Items) != 2 || resp.Items[1].Key != "pf-2" {
		t.Fatalf("items = %+v, want [pf-1 pf-2]", resp.Items)
	}
	if resp.Next != "pf-2" {
		t.Errorf("Next = %q, want pf-2 (page cut at limit)", resp.Next)
	}
}

// TestScanKeysLocalModeStoreError: a failing store read surfaces as 500 from
// the local scan endpoint (and thus fails a coordinator's scatter closed).
func TestScanKeysLocalModeStoreError(t *testing.T) {
	dir := t.TempDir()
	s := store.New()
	s.SetFlushPolicy(dir, 0)
	t.Cleanup(func() { _ = s.CloseTables() })
	if err := s.Put("le-a", "v", store.VectorClockVersion{Clocks: clk("w", 1)}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no table files: %v", err)
	}
	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.WriteFile(files[0], make([]byte, info.Size()), 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	r := ring.NewRing(10)
	ml := gossip.NewMemberList("self", "localhost", nil)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	req := httptest.NewRequest(http.MethodGet, "/keys?prefix=le-", nil)
	req.Header.Set(headerXProxiedFrom, "test-peer")
	w := httptest.NewRecorder()
	h.ScanKeys(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("local scan over corrupted table: got %d, want 500", w.Code)
	}
}

// TestScanKeysPaginationWalksAll drives the coordinator against the real
// local store (self only) and pages through with after= until Next is empty.
func TestScanKeysPaginationWalksAll(t *testing.T) {
	h, _ := newScanHandler(t)
	const n = 7
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("pw-%02d", i)
		if err := h.store.Put(key, "v", store.VectorClockVersion{Clocks: clk("w", uint64(i+1))}); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	// Delete one mid-range key; pagination must skip it without losing others.
	if err := h.store.Delete("pw-03", store.VectorClockVersion{Clocks: clk("w", 100)}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var got []string
	after := ""
	for {
		w, resp := doScan(t, h, "prefix=pw-&limit=3&after="+after, false)
		if w.Code != http.StatusOK {
			t.Fatalf("scan page: got %d: %s", w.Code, w.Body.String())
		}
		for _, it := range resp.Items {
			got = append(got, it.Key)
		}
		if resp.Next == "" {
			break
		}
		after = resp.Next
	}
	want := []string{"pw-00", "pw-01", "pw-02", "pw-04", "pw-05", "pw-06"}
	if len(got) != len(want) {
		t.Fatalf("pagination visited %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pagination visited %v, want %v", got, want)
		}
	}
}

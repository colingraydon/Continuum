package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/store"
)

const (
	scanDefaultLimit = 100
	scanMaxLimit     = 1000
)

// ScanItem is one key's sibling set in a node-local scan response. Tombstone
// siblings are included so the coordinator can merge dominance across nodes.
type ScanItem struct {
	Key      string            `json:"key"`
	Siblings []SiblingResponse `json:"siblings"`
}

// ScanResponse is the local-mode response to GET /keys (X-Proxied-From set).
type ScanResponse struct {
	Items []ScanItem `json:"items"`
}

// ScanResult is one key in the client-facing coordinator response: a single
// live value, or a siblings list when concurrent writes exist.
type ScanResult struct {
	Key      string            `json:"key"`
	Value    string            `json:"value,omitempty"`
	Siblings []SiblingResponse `json:"siblings,omitempty"`
}

// ScanKeysResponse is the client-facing response to GET /keys?prefix=.
// Next is the exclusive cursor to resume from; empty means the scan is done.
type ScanKeysResponse struct {
	Items []ScanResult `json:"items"`
	Next  string       `json:"next,omitempty"`
}

// scanParams are the validated query parameters shared by both scan modes.
type scanParams struct {
	prefix, after string
	limit         int
}

func parseScanParams(req *http.Request) (scanParams, error) {
	q := req.URL.Query()
	p := scanParams{prefix: q.Get("prefix"), after: q.Get("after"), limit: scanDefaultLimit}
	if p.prefix == "" {
		return p, fmt.Errorf("prefix param required")
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return p, fmt.Errorf("invalid limit %q", raw)
		}
		p.limit = min(n, scanMaxLimit)
	}
	return p, nil
}

// ScanKeys serves GET /keys?prefix= - an ordered prefix scan. With
// X-Proxied-From set it scans the local store only; otherwise it coordinates
// a scatter-gather across every alive member and merges per-key sibling sets
// with the same vector-clock dominance rules as point reads.
func (h *Handler) ScanKeys(w http.ResponseWriter, req *http.Request) {
	params, err := parseScanParams(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Header.Get(headerXProxiedFrom) != "" {
		h.localScan(w, params)
		return
	}
	if m, ok := h.memberList.Get(h.selfID); ok && m.Bootstrapping {
		http.Error(w, errNodeBootstrapping, http.StatusServiceUnavailable)
		return
	}
	h.coordinatorScan(w, params)
}

// localScan answers with this node's own visible entries for the range.
func (h *Handler) localScan(w http.ResponseWriter, params scanParams) {
	items, err := h.store.Scan(params.prefix, params.after, params.limit)
	if err != nil {
		log.Printf("scan: local scan failed: %v", err)
		http.Error(w, errLocalRead, http.StatusInternalServerError)
		return
	}
	resp := ScanResponse{Items: keyItemsToScanItems(items)}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// keyItemsToScanItems converts store scan results to the wire sibling format.
func keyItemsToScanItems(items []store.KeyItem) []ScanItem {
	out := make([]ScanItem, len(items))
	for i, it := range items {
		sibs := make([]SiblingResponse, len(it.Entry.Siblings))
		for j, sib := range it.Entry.Siblings {
			sibs[j] = SiblingResponse{Value: sib.Value, Deleted: sib.Deleted, Clocks: sib.Version.Clocks}
		}
		out[i] = ScanItem{Key: it.Key, Siblings: sibs}
	}
	return out
}

// nodeScanResult is one member's local scan outcome during scatter-gather.
type nodeScanResult struct {
	nodeID string
	items  []ScanItem
	err    error
}

// coordinatorScan fans the scan to every alive, non-bootstrapping member
// (including self) and merges the results. Any member failure fails the scan:
// a partial result silently missing that member's keys is worse than an error.
func (h *Handler) coordinatorScan(w http.ResponseWriter, params scanParams) {
	members := h.scanTargets()
	results := make(chan nodeScanResult, len(members))
	for _, m := range members {
		go func(m *gossip.Member) {
			if m.ID == h.selfID {
				items, err := h.store.Scan(params.prefix, params.after, params.limit)
				results <- nodeScanResult{nodeID: m.ID, items: keyItemsToScanItems(items), err: err}
				return
			}
			items, err := h.remoteScan(m.Address, params)
			results <- nodeScanResult{nodeID: m.ID, items: items, err: err}
		}(m)
	}

	collected := make([]nodeScanResult, 0, len(members))
	for range members {
		r := <-results
		if r.err != nil {
			log.Printf("scan: node %s failed: %v", r.nodeID, r.err)
			http.Error(w, "scan requires all alive nodes; node "+r.nodeID+" failed", http.StatusServiceUnavailable)
			return
		}
		collected = append(collected, r)
	}

	resp := mergeScanResults(collected, params.limit)
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// scanTargets returns every alive, non-bootstrapping member. Bootstrapping
// nodes are excluded for the same reason as point reads: they do not yet hold
// complete data, and the keys they will own are still readable elsewhere.
func (h *Handler) scanTargets() []*gossip.Member {
	alive := h.memberList.GetAlive()
	out := make([]*gossip.Member, 0, len(alive))
	for _, m := range alive {
		if !m.Bootstrapping {
			out = append(out, m)
		}
	}
	return out
}

// remoteScan runs a local-mode scan on a peer.
func (h *Handler) remoteScan(address string, params scanParams) ([]ScanItem, error) {
	u := fmt.Sprintf("%s%s/keys?prefix=%s&after=%s&limit=%d",
		schemeHTTP, address, url.QueryEscape(params.prefix), url.QueryEscape(params.after), params.limit)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(headerXProxiedFrom, h.selfID)
	resp, err := h.replicaClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scan: peer returned %d", resp.StatusCode)
	}
	var sr ScanResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	return sr.Items, nil
}

// mergeScanResults merges per-node scan pages into the client response.
//
// Each node returns its smallest `limit` matches (tombstones included), so a
// key can only be trusted as fully merged if it is <= every full page's last
// key - beyond that horizon a node may hold more data the page cut off. Keys
// past the horizon are deferred to the next page via the cursor. Within the
// horizon: per-key sibling sets merge under vector-clock dominance
// (mergeResponses), keys whose lone survivor is a tombstone are dropped, and
// the page is cut at `limit`. Next is the exclusive resume cursor: the last
// emitted key when the page filled, otherwise the horizon; empty means done.
func mergeScanResults(collected []nodeScanResult, limit int) ScanKeysResponse {
	horizon, bounded := scanHorizon(collected, limit)

	perKey := make(map[string][]NodeResponse)
	for _, r := range collected {
		for _, item := range r.items {
			if bounded && item.Key > horizon {
				continue
			}
			perKey[item.Key] = append(perKey[item.Key], NodeResponse{ID: r.nodeID, Siblings: item.Siblings})
		}
	}

	keys := make([]string, 0, len(perKey))
	for k := range perKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var resp ScanKeysResponse
	for _, k := range keys {
		if len(resp.Items) == limit {
			// Page full: resume right after the last emitted key.
			resp.Next = resp.Items[len(resp.Items)-1].Key
			return resp
		}
		survivors := mergeResponses(perKey[k])
		if len(survivors) == 1 && survivors[0].Deleted {
			continue // fully deleted key
		}
		resp.Items = append(resp.Items, toScanResult(k, survivors))
	}
	if bounded {
		// Every in-horizon key was emitted or dropped; more may exist beyond.
		resp.Next = horizon
	}
	return resp
}

// scanHorizon returns the largest key that is guaranteed fully merged: the
// smallest last-key among nodes whose page filled (a full page may have cut
// off later keys). bounded=false means every node was exhausted and the whole
// range is trusted.
func scanHorizon(collected []nodeScanResult, limit int) (string, bool) {
	horizon, bounded := "", false
	for _, r := range collected {
		if len(r.items) < limit {
			continue // node exhausted its range; no cutoff
		}
		last := r.items[len(r.items)-1].Key
		if !bounded || last < horizon {
			horizon, bounded = last, true
		}
	}
	return horizon, bounded
}

func toScanResult(key string, survivors []SiblingResponse) ScanResult {
	if len(survivors) == 1 && !survivors[0].Deleted {
		return ScanResult{Key: key, Value: survivors[0].Value}
	}
	return ScanResult{Key: key, Siblings: survivors}
}

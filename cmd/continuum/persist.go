package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/colingraydon/continuum/internal/wal"
)

// On-disk layout under DATA_DIR:
//
//   meta.json               identity + last_clean_shutdown + latest_seq
//   incarnation             this node's gossip epoch, advanced on each restart
//   tables/NNNNNNNN.sst     immutable SSTables, named by the WAL sequence
//                           they cover (highest = replay skip threshold)
//   tables/NNNNNNNN.sst.tmp in-flight flush (cleaned at startup)
//   wal/NNNNNNNN.wal        segmented write-ahead log
//
//   snap/NNNNNNNN.snap      legacy (pre-LSM) snapshot; migrated to an
//                           SSTable on first startup, then removed
//
// recoverStore() runs on startup. finalize() runs on graceful shutdown.

const (
	metaFile        = "meta.json"
	snapDirName     = "snap"
	walDirName      = "wal"
	tablesDirName   = "tables"
	snapSuffix      = ".snap"
	snapTmpSuffix   = ".snap.tmp"
	incarnationFile = "incarnation"
)

type persistMeta struct {
	NodeID            string    `json:"node_id"`
	LastCleanShutdown time.Time `json:"last_clean_shutdown"`
	LatestSeq         uint64    `json:"latest_seq"`
}

// persistence holds the open WAL writer and dir paths so finalize can
// reuse them at shutdown.
type persistence struct {
	dataDir string
	nodeID  string
	s       *store.Store
	w       *wal.Writer
	// discardedData is set when the downtime gate threw away non-empty
	// local storage. The node then rejoins as bootstrapping: its store can
	// no longer vouch for the keys it replicates, so it must stay out of
	// read sets and CAS quorums until it has pulled its replica ranges back
	// (fault-harness finding #10). A first-ever start (nothing to discard)
	// does not set it.
	discardedData bool
}

// recoverStore opens dataDir, applies the downtime gate, attaches SSTables
// (migrating a legacy snapshot if one is present), replays the WAL tail, and
// returns the populated store plus a persistence handle for shutdown.
// memtableMaxBytes sets the flush threshold; blockCacheBytes sizes the
// shared SSTable block cache (<= 0 disables it). If dataDir is empty,
// persistence is disabled and a fresh in-memory store is returned with nil
// persistence.
func recoverStore(dataDir, nodeID string, gcTTL time.Duration, memtableMaxBytes, blockCacheBytes int64) (*store.Store, *persistence, error) {
	if dataDir == "" {
		return store.New(), nil, nil
	}

	walDir, tablesDir, err := setupDataDirs(dataDir)
	if err != nil {
		return nil, nil, err
	}
	snapDir := filepath.Join(dataDir, snapDirName)

	m, hasMeta, err := readPersistMeta(filepath.Join(dataDir, metaFile))
	if err != nil {
		return nil, nil, err
	}
	if hasMeta && m.NodeID != "" && m.NodeID != nodeID {
		return nil, nil, fmt.Errorf("persist: data dir owned by %q, not %q", m.NodeID, nodeID)
	}

	s := store.New()
	// The cache must be installed before OpenTables so every reader — restored,
	// flushed, or compacted — is attached to it.
	s.SetBlockCache(sstable.NewCache(blockCacheBytes))
	p := &persistence{dataDir: dataDir, nodeID: nodeID, s: s}

	if downtimeGateFired(m, hasMeta, gcTTL) {
		logDowntimeGate(m, hasMeta, gcTTL)
		p.discardedData = dirsHaveFiles(snapDir, walDir, tablesDir)
		if err := clearStorageFiles(snapDir, walDir, tablesDir); err != nil {
			return nil, nil, err
		}
		return openWALAndReturn(s, p, walDir, tablesDir, memtableMaxBytes)
	}

	skipBelow, migrate, err := loadTablesOrSnapshot(s, tablesDir, snapDir, nodeID)
	if err != nil {
		return nil, nil, err
	}
	if err := replayWALSegments(s, walDir, skipBelow); err != nil {
		return nil, nil, err
	}
	if _, _, err := openWALAndReturn(s, p, walDir, tablesDir, memtableMaxBytes); err != nil {
		return nil, nil, err
	}
	if migrate {
		if err := finishSnapshotMigration(s, snapDir); err != nil {
			return nil, nil, err
		}
	}
	return s, p, nil
}

// loadTablesOrSnapshot attaches existing SSTables and returns the WAL
// replay-skip threshold. For a pre-LSM data dir (no tables but a snapshot
// present) it instead loads the snapshot into the memtable and reports
// migrate=true, so the caller flushes it out as the first SSTable after the
// WAL opens.
func loadTablesOrSnapshot(s *store.Store, tablesDir, snapDir, nodeID string) (skipBelow uint64, migrate bool, err error) {
	skipBelow, err = s.OpenTables(tablesDir)
	if err != nil {
		return 0, false, err
	}
	if s.TableCount() > 0 {
		return skipBelow, false, nil
	}
	snapSeq, loaded, err := loadSnapshotIfPresent(s, snapDir, nodeID)
	if err != nil {
		return 0, false, err
	}
	if loaded {
		return snapSeq, true, nil
	}
	return skipBelow, false, nil
}

// finishSnapshotMigration flushes the migrated snapshot to its first SSTable
// and removes the legacy snapshot files.
func finishSnapshotMigration(s *store.Store, snapDir string) error {
	if err := s.Flush(); err != nil {
		return fmt.Errorf("persist: migrate snapshot to sstable: %w", err)
	}
	if err := clearDirFiles(snapDir); err != nil {
		return err
	}
	log.Printf("persist: migrated legacy snapshot to sstable")
	return nil
}

func setupDataDirs(dataDir string) (walDir, tablesDir string, err error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", fmt.Errorf("persist: mkdir %s: %w", dataDir, err)
	}
	walDir = filepath.Join(dataDir, walDirName)
	tablesDir = filepath.Join(dataDir, tablesDirName)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(tablesDir, 0o755); err != nil {
		return "", "", err
	}
	if err := cleanupSnapTmp(filepath.Join(dataDir, snapDirName)); err != nil {
		return "", "", err
	}
	return walDir, tablesDir, nil
}

func downtimeGateFired(m persistMeta, hasMeta bool, gcTTL time.Duration) bool {
	return !hasMeta || m.LastCleanShutdown.IsZero() || time.Since(m.LastCleanShutdown) > gcTTL
}

func logDowntimeGate(m persistMeta, hasMeta bool, gcTTL time.Duration) {
	if hasMeta {
		log.Printf("persist: last clean shutdown was %v ago > GCTTL %v; discarding local data and re-bootstrapping",
			time.Since(m.LastCleanShutdown).Round(time.Second), gcTTL)
	} else {
		log.Printf("persist: no prior meta; starting fresh")
	}
}

func loadSnapshotIfPresent(s *store.Store, snapDir, nodeID string) (uint64, bool, error) {
	snapPath, err := findLatestSnapshot(snapDir)
	if err != nil {
		return 0, false, err
	}
	if snapPath == "" {
		return 0, false, nil
	}
	f, err := os.Open(snapPath)
	if err != nil {
		return 0, false, fmt.Errorf("persist: open snapshot: %w", err)
	}
	hdr, err := s.LoadSnapshot(f, nodeID)
	_ = f.Close()
	if err != nil {
		return 0, false, fmt.Errorf("persist: load snapshot: %w", err)
	}
	log.Printf("persist: loaded legacy snapshot epoch=%d sequence_at=%d entries=%d",
		hdr.Epoch, hdr.SequenceAt, hdr.EntryCount)
	return hdr.SequenceAt, true, nil
}

func replayWALSegments(s *store.Store, walDir string, skipBelow uint64) error {
	r, err := wal.NewReader(walDir)
	if err != nil {
		return err
	}
	lastSeq, err := s.Replay(r, skipBelow)
	_ = r.Close()
	if err != nil {
		return fmt.Errorf("persist: replay: %w", err)
	}
	if r.TornTail() {
		log.Printf("persist: torn tail detected at WAL end; will be truncated on next open")
	}
	if lastSeq > skipBelow {
		log.Printf("persist: replayed WAL through seq %d (skipped <= %d)", lastSeq, skipBelow)
	}
	return nil
}

func openWALAndReturn(s *store.Store, p *persistence, walDir, tablesDir string, memtableMaxBytes int64) (*store.Store, *persistence, error) {
	w, err := wal.Open(walDir)
	if err != nil {
		return nil, nil, err
	}
	p.w = w
	s.SetWAL(w)
	s.SetFlushPolicy(tablesDir, memtableMaxBytes)
	return s, p, nil
}

// finalize flushes the memtable to a final SSTable (which truncates covered
// WAL segments), closes the table readers and the WAL, and updates meta with
// last_clean_shutdown. Safe to call on a nil receiver — useful for
// memory-only mode.
func (p *persistence) finalize() error {
	if p == nil {
		return nil
	}
	if err := p.s.Flush(); err != nil {
		return fmt.Errorf("persist: final flush: %w", err)
	}
	if err := p.s.CloseTables(); err != nil {
		return fmt.Errorf("persist: close tables: %w", err)
	}
	if err := p.w.Close(); err != nil {
		return fmt.Errorf("persist: close wal: %w", err)
	}
	next := persistMeta{
		NodeID:            p.nodeID,
		LastCleanShutdown: time.Now(),
		LatestSeq:         p.s.LastSeq(),
	}
	if err := writePersistMeta(filepath.Join(p.dataDir, metaFile), next); err != nil {
		return fmt.Errorf("persist: write meta: %w", err)
	}
	return nil
}

func readPersistMeta(path string) (persistMeta, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persistMeta{}, false, nil
	}
	if err != nil {
		return persistMeta{}, false, fmt.Errorf("persist: read meta: %w", err)
	}
	var m persistMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return persistMeta{}, false, fmt.Errorf("persist: parse meta: %w", err)
	}
	return m, true, nil
}

func writePersistMeta(path string, m persistMeta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(filepath.Dir(path))
}

func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

// incarnationStore persists this node's gossip incarnation (epoch) across
// restarts, Cassandra-generation style. Refutation lets a crash-restarted node
// relearn its epoch from what peers remember, but that needs an inbound gossip
// a node buried as dead might never receive. A persisted incarnation makes the
// node self-sufficient: on restart it loads the last value, advances past it,
// and its very first gossip already dominates any stale entry — no peer round
// trip required. The file holds a single decimal uint64.
type incarnationStore struct {
	path string
	mu   sync.Mutex
}

func newIncarnationStore(dataDir string) *incarnationStore {
	return &incarnationStore{path: filepath.Join(dataDir, incarnationFile)}
}

// load returns the last persisted incarnation, or 0 if none is stored yet or
// the file is missing/unreadable/corrupt — all treated as a fresh start, which
// is safe because a lower-than-remembered value simply falls back to refutation.
func (s *incarnationStore) load() uint64 {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// store atomically persists v (temp + fsync + rename + dir fsync) so a crash
// mid-write cannot leave a torn value.
func (s *incarnationStore) store(v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(v, 10)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(filepath.Dir(s.path))
}

func findLatestSnapshot(snapDir string) (string, error) {
	entries, err := os.ReadDir(snapDir)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("persist: read snap dir: %w", err)
	}
	var snaps []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), snapSuffix) {
			continue
		}
		snaps = append(snaps, e.Name())
	}
	if len(snaps) == 0 {
		return "", nil
	}
	sort.Strings(snaps)
	return filepath.Join(snapDir, snaps[len(snaps)-1]), nil
}

func cleanupSnapTmp(snapDir string) error {
	entries, err := os.ReadDir(snapDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), snapTmpSuffix) {
			_ = os.Remove(filepath.Join(snapDir, e.Name()))
		}
	}
	return nil
}

// dirsHaveFiles reports whether any of the directories contains at least one
// regular file — i.e. whether a downtime-gate clear actually discards data.
func dirsHaveFiles(dirs ...string) bool {
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				return true
			}
		}
	}
	return false
}

func clearStorageFiles(dirs ...string) error {
	for _, dir := range dirs {
		if err := clearDirFiles(dir); err != nil {
			return err
		}
	}
	return nil
}

func clearDirFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("persist: remove %s: %w", e.Name(), err)
		}
	}
	return nil
}

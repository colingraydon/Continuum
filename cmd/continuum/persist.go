package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/colingraydon/continuum/internal/store"
	"github.com/colingraydon/continuum/internal/wal"
)

// On-disk layout under DATA_DIR:
//
//   meta.json               identity + last_clean_shutdown + latest_seq
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
	metaFile      = "meta.json"
	snapDirName   = "snap"
	walDirName    = "wal"
	tablesDirName = "tables"
	snapSuffix    = ".snap"
	snapTmpSuffix = ".snap.tmp"
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
}

// recoverStore opens dataDir, applies the downtime gate, attaches SSTables
// (migrating a legacy snapshot if one is present), replays the WAL tail, and
// returns the populated store plus a persistence handle for shutdown.
// memtableMaxBytes sets the flush threshold. If dataDir is empty,
// persistence is disabled and a fresh in-memory store is returned with nil
// persistence.
func recoverStore(dataDir, nodeID string, gcTTL time.Duration, memtableMaxBytes int64) (*store.Store, *persistence, error) {
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
	p := &persistence{dataDir: dataDir, nodeID: nodeID, s: s}

	if downtimeGateFired(m, hasMeta, gcTTL) {
		logDowntimeGate(m, hasMeta, gcTTL)
		if err := clearStorageFiles(snapDir, walDir, tablesDir); err != nil {
			return nil, nil, err
		}
		return openWALAndReturn(s, p, walDir, tablesDir, memtableMaxBytes)
	}

	skipBelow, err := s.OpenTables(tablesDir)
	if err != nil {
		return nil, nil, err
	}

	// Legacy migration: a pre-LSM data dir has a snapshot and no tables.
	// Load it as memtable contents; after the WAL is open it gets flushed
	// out as the first SSTable and the snapshot files are removed.
	migrate := false
	if s.TableCount() == 0 {
		snapSeq, loaded, err := loadSnapshotIfPresent(s, snapDir, nodeID)
		if err != nil {
			return nil, nil, err
		}
		if loaded {
			skipBelow = snapSeq
			migrate = true
		}
	}

	if err := replayWALSegments(s, walDir, skipBelow); err != nil {
		return nil, nil, err
	}
	if _, _, err := openWALAndReturn(s, p, walDir, tablesDir, memtableMaxBytes); err != nil {
		return nil, nil, err
	}
	if migrate {
		if err := s.Flush(); err != nil {
			return nil, nil, fmt.Errorf("persist: migrate snapshot to sstable: %w", err)
		}
		if err := clearDirFiles(snapDir); err != nil {
			return nil, nil, err
		}
		log.Printf("persist: migrated legacy snapshot to sstable")
	}
	return s, p, nil
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

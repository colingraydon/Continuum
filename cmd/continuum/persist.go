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
//   meta.json              identity + last_clean_shutdown + snapshot_epoch
//   snap/NNNNNNNN.snap     latest durable snapshot
//   snap/NNNNNNNN.snap.tmp in-flight snapshot (cleaned at startup)
//   wal/NNNNNNNN.wal       segmented write-ahead log
//
// recover() runs on startup. finalize() runs on graceful shutdown.

const (
	metaFile      = "meta.json"
	snapDirName   = "snap"
	walDirName    = "wal"
	snapSuffix    = ".snap"
	snapTmpSuffix = ".snap.tmp"
)

type persistMeta struct {
	NodeID            string    `json:"node_id"`
	LastCleanShutdown time.Time `json:"last_clean_shutdown"`
	SnapshotEpoch     uint64    `json:"snapshot_epoch"`
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

// recoverStore opens dataDir, applies the downtime gate, loads the latest
// snapshot, replays the WAL, and returns the populated store plus a
// persistence handle for shutdown. If dataDir is empty, persistence is
// disabled and a fresh in-memory store is returned with nil persistence.
func recoverStore(dataDir, nodeID string, gcTTL time.Duration) (*store.Store, *persistence, error) {
	if dataDir == "" {
		return store.New(), nil, nil
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("persist: mkdir %s: %w", dataDir, err)
	}
	snapDir := filepath.Join(dataDir, snapDirName)
	walDir := filepath.Join(dataDir, walDirName)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, nil, err
	}
	if err := cleanupSnapTmp(snapDir); err != nil {
		return nil, nil, err
	}

	m, hasMeta, err := readPersistMeta(filepath.Join(dataDir, metaFile))
	if err != nil {
		return nil, nil, err
	}
	if hasMeta && m.NodeID != "" && m.NodeID != nodeID {
		return nil, nil, fmt.Errorf("persist: data dir owned by %q, not %q", m.NodeID, nodeID)
	}

	s := store.New()
	p := &persistence{dataDir: dataDir, nodeID: nodeID, s: s}

	// Downtime gate.
	if !hasMeta || m.LastCleanShutdown.IsZero() || time.Since(m.LastCleanShutdown) > gcTTL {
		if hasMeta {
			log.Printf("persist: last clean shutdown was %v ago > GCTTL %v; discarding local data and re-bootstrapping",
				time.Since(m.LastCleanShutdown).Round(time.Second), gcTTL)
		} else {
			log.Printf("persist: no prior meta; starting fresh")
		}
		if err := clearStorageFiles(snapDir, walDir); err != nil {
			return nil, nil, err
		}
		w, err := wal.Open(walDir)
		if err != nil {
			return nil, nil, err
		}
		p.w = w
		s.SetWAL(w)
		return s, p, nil
	}

	// Load latest snapshot, if any.
	var skipBelow uint64
	snapPath, err := findLatestSnapshot(snapDir)
	if err != nil {
		return nil, nil, err
	}
	if snapPath != "" {
		f, err := os.Open(snapPath)
		if err != nil {
			return nil, nil, fmt.Errorf("persist: open snapshot: %w", err)
		}
		hdr, err := s.LoadSnapshot(f, nodeID)
		_ = f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("persist: load snapshot: %w", err)
		}
		skipBelow = hdr.SequenceAt
		log.Printf("persist: loaded snapshot epoch=%d sequence_at=%d entries=%d",
			hdr.Epoch, hdr.SequenceAt, hdr.EntryCount)
	}

	// Replay WAL records past the snapshot.
	r, err := wal.NewReader(walDir)
	if err != nil {
		return nil, nil, err
	}
	lastSeq, err := s.Replay(r, skipBelow)
	_ = r.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("persist: replay: %w", err)
	}
	if r.TornTail() {
		log.Printf("persist: torn tail detected at WAL end; will be truncated on next open")
	}
	if lastSeq > skipBelow {
		log.Printf("persist: replayed WAL through seq %d (skipped <= %d)", lastSeq, skipBelow)
	}

	// Open WAL writer for live traffic.
	w, err := wal.Open(walDir)
	if err != nil {
		return nil, nil, err
	}
	p.w = w
	s.SetWAL(w)
	return s, p, nil
}

// finalize takes a final snapshot, writes a CHECKPOINT, truncates covered
// WAL segments, closes the WAL, and updates meta with last_clean_shutdown.
// Safe to call on a nil receiver — useful for memory-only mode.
func (p *persistence) finalize() error {
	if p == nil {
		return nil
	}
	prior, _, _ := readPersistMeta(filepath.Join(p.dataDir, metaFile))
	nextEpoch := prior.SnapshotEpoch + 1
	// nextSeq returns the seq the next Append would use; the highest applied
	// is one less. NextSeq is at least 1 even with no writes, so subtract.
	seq := p.w.NextSeq() - 1

	snapDir := filepath.Join(p.dataDir, snapDirName)
	snapName := fmt.Sprintf("%020d%s", nextEpoch, snapSuffix)
	finalPath := filepath.Join(snapDir, snapName)
	tmpPath := finalPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("persist: create snap.tmp: %w", err)
	}
	hdr := store.SnapHeader{NodeID: p.nodeID, Epoch: nextEpoch, SequenceAt: seq}
	if err := p.s.Snapshot(f, hdr); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist: write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist: sync snap.tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist: close snap.tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("persist: rename snap: %w", err)
	}
	if err := fsyncDir(snapDir); err != nil {
		return fmt.Errorf("persist: fsync snap dir: %w", err)
	}

	// Anchor the WAL with a CHECKPOINT, then drop segments the snapshot covers.
	if err := p.s.WriteCheckpoint(seq); err != nil {
		return fmt.Errorf("persist: write checkpoint: %w", err)
	}
	if err := p.w.TruncateThrough(seq); err != nil {
		return fmt.Errorf("persist: truncate wal: %w", err)
	}
	if err := p.w.Close(); err != nil {
		return fmt.Errorf("persist: close wal: %w", err)
	}

	next := persistMeta{
		NodeID:            p.nodeID,
		LastCleanShutdown: time.Now(),
		SnapshotEpoch:     nextEpoch,
		LatestSeq:         seq,
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

func clearStorageFiles(snapDir, walDir string) error {
	if err := clearDirFiles(snapDir); err != nil {
		return err
	}
	return clearDirFiles(walDir)
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

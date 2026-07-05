package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// BackupInfo summarizes a completed backup.
type BackupInfo struct {
	Tables int    // number of SSTables captured
	MaxSeq uint64 // highest WAL sequence the backup covers
}

// Backup writes a consistent point-in-time copy of the store's on-disk table
// set into destDir. It first flushes the active memtable so recent writes are
// captured on disk, then hard-links every live SSTable into destDir and writes
// a matching manifest. Because SSTables are immutable and the copy is by hard
// link, a backup is near-instant and space-free (it shares inodes with the
// live tables until one side is compacted away).
//
// destDir must be on the same filesystem as the data directory (hard links do
// not cross devices) and must not already hold a backup. Restore is the
// existing recovery path: point a fresh store's OpenTables at destDir, or copy
// its contents into a data directory's tables folder.
//
// Backup requires flushing to be enabled (SetFlushPolicy); a memory-only store
// keeps its data in the memtable, which a backup of the table set cannot see.
func (s *Store) Backup(destDir string) (BackupInfo, error) {
	if destDir == "" {
		return BackupInfo{}, errors.New("store: backup destination is empty")
	}
	s.mu.RLock()
	srcDir := s.flushDir
	s.mu.RUnlock()
	if srcDir == "" {
		return BackupInfo{}, errors.New("store: backup requires flushing to be enabled")
	}
	// Flush outside any lock: it takes s.mu itself. After it returns, the
	// memtable's contents are durably in an SSTable named by the manifest.
	if err := s.Flush(); err != nil {
		return BackupInfo{}, fmt.Errorf("store: flush before backup: %w", err)
	}
	if err := prepareBackupDir(destDir); err != nil {
		return BackupInfo{}, err
	}
	m, err := s.linkLiveTables(destDir)
	if err != nil {
		return BackupInfo{}, err
	}
	// writeManifest fsyncs destDir, which also persists the hard-link entries
	// created above, so the backup is durable as a unit once it returns.
	if err := writeManifest(destDir, m); err != nil {
		return BackupInfo{}, err
	}
	return BackupInfo{Tables: len(m.Tables), MaxSeq: m.MaxSeq}, nil
}

// prepareBackupDir creates destDir and refuses to write over an existing
// backup (one whose manifest is already present), so two backups never mix
// their table sets in one directory.
func prepareBackupDir(destDir string) error {
	_, err := os.Stat(filepath.Join(destDir, manifestFile))
	if err == nil {
		return fmt.Errorf("store: backup dir %q already holds a backup", destDir)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: stat backup dir: %w", err)
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("store: create backup dir: %w", err)
	}
	return nil
}

// linkLiveTables hard-links the current live table set into destDir under
// s.mu held for reading. Holding the lock across the links serializes against
// compaction: a retired table is unlinked only after a new manifest excluding
// it is committed under s.mu, which cannot happen while this read lock is held.
func (s *Store) linkLiveTables(destDir string) (manifest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, len(s.tables))
	for i, t := range s.tables {
		names[i] = t.file
		src := filepath.Join(s.flushDir, t.file)
		dst := filepath.Join(destDir, t.file)
		if err := os.Link(src, dst); err != nil {
			return manifest{}, fmt.Errorf("store: link table %s: %w", t.file, err)
		}
	}
	return manifest{Tables: names, MaxSeq: s.maxTableSeq, NextFileNum: s.nextFileNum}, nil
}

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// manifestFile names the table set's source of truth inside the tables
// directory. It records which SSTables are live, their recency order, the
// highest WAL sequence they cover, and the next table file number to hand
// out. Flush and compaction rewrite it atomically; OpenTables reads it.
const manifestFile = "MANIFEST"

// manifest is the on-disk record of the live table set. Tables are listed
// newest first — the same order reads probe them — so a key's freshest copy
// is always found before any stale shadowed copy below it.
type manifest struct {
	Tables      []string `json:"tables"`        // base filenames, newest first
	MaxSeq      uint64   `json:"max_seq"`       // highest WAL sequence any table covers
	NextFileNum uint64   `json:"next_file_num"` // next table file number to allocate
}

// readManifest loads the manifest from dir. The second return is false when
// no manifest exists yet (a pre-compaction data dir, or a brand-new one),
// which callers handle by migrating from the raw .sst listing.
func readManifest(dir string) (manifest, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if errors.Is(err, os.ErrNotExist) {
		return manifest{}, false, nil
	}
	if err != nil {
		return manifest{}, false, fmt.Errorf("store: read manifest: %w", err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, false, fmt.Errorf("store: parse manifest: %w", err)
	}
	return m, true, nil
}

// writeManifest atomically replaces the manifest in dir: write a temp file,
// fsync it, rename over the old manifest, then fsync the directory so the
// rename is durable. A crash leaves either the old or the new manifest, never
// a partial one — this is the atomic commit point for both flush and
// compaction.
func writeManifest(dir string, m manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode manifest: %w", err)
	}
	tmp := filepath.Join(dir, manifestFile+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("store: create manifest tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("store: write manifest tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("store: sync manifest tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: close manifest tmp: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestFile)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: rename manifest: %w", err)
	}
	return fsyncDir(dir)
}

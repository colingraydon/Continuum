package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/colingraydon/continuum/internal/fsutil"
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

// writeManifest atomically replaces the manifest in dir (temp file, fsync,
// rename, directory fsync via fsutil.WriteFileAtomic). A crash leaves either
// the old or the new manifest, never a partial one — this is the atomic
// commit point for both flush and compaction.
func writeManifest(dir string, m manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode manifest: %w", err)
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(dir, manifestFile), data); err != nil {
		return fmt.Errorf("store: write manifest: %w", err)
	}
	return nil
}

// Package fsutil holds the small crash-safe filesystem primitives shared by
// the persistence layers (table manifest, Merkle snapshots).
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic replaces path with data crash-safely: write a temp file in
// the same directory, fsync it, rename it over path, then fsync the directory
// so the rename is durable. A crash leaves either the old file or the new
// one, never a partial write - this is the atomic commit point used by the
// store manifest and the anti-entropy Merkle snapshot.
func WriteFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("fsutil: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsutil: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsutil: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("fsutil: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("fsutil: rename %s: %w", path, err)
	}
	return SyncDir(filepath.Dir(path))
}

// SyncDir fsyncs a directory so a rename or file creation within it is
// durable across a crash.
func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

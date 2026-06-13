package store

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	if _, ok, err := readManifest(dir); ok || err != nil {
		t.Fatalf("readManifest on empty dir = (ok=%v, err=%v), want (false, nil)", ok, err)
	}

	m := manifest{Tables: []string{tableName(2), tableName(1)}, MaxSeq: 42, NextFileNum: 3}
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	got, ok, err := readManifest(dir)
	if err != nil || !ok {
		t.Fatalf("readManifest = (ok=%v, err=%v)", ok, err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("round-trip = %+v, want %+v", got, m)
	}
}

func TestReadManifestGarbage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, manifestFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readManifest(dir); err == nil {
		t.Fatal("readManifest should error on malformed JSON")
	}
}

func TestWriteManifestCreateError(t *testing.T) {
	dir := t.TempDir()
	// A directory occupying the temp path makes os.Create fail.
	if err := os.Mkdir(filepath.Join(dir, manifestFile+".tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeManifest(dir, manifest{NextFileNum: 1}); err == nil {
		t.Fatal("writeManifest should error when the temp path is unwritable")
	}
}

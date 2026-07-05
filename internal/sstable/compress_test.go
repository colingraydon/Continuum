package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/twmb/murmur3"
)

// compressibleEntries returns n entries whose values are long runs of
// repeated text, so every block compresses well.
func compressibleEntries(n int) []kv {
	out := make([]kv, n)
	for i := range out {
		out[i] = kv{
			key:   []byte(fmt.Sprintf("key-%05d", i)),
			value: bytes.Repeat([]byte("lorem ipsum dolor sit amet "), 8),
		}
	}
	return out
}

// incompressibleEntries returns n entries with pseudo-random values that s2
// cannot shrink, forcing the per-block raw fallback.
func incompressibleEntries(n int) []kv {
	rng := uint64(0x9E3779B97F4A7C15)
	out := make([]kv, n)
	for i := range out {
		value := make([]byte, 200)
		for j := range value {
			rng = rng*6364136223846793005 + 1442695040888963407
			value[j] = byte(rng >> 33)
		}
		out[i] = kv{key: []byte(fmt.Sprintf("key-%05d", i)), value: value}
	}
	return out
}

// blockTypes decodes the footer and index of a finished table and returns
// each data block's type byte (the byte just before the CRC trailer).
func blockTypes(t *testing.T, data []byte) []byte {
	t.Helper()
	f, err := decodeFooter(data[len(data)-footerSize:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	index, _, err := decodeIndexBlock(data[f.indexOff : f.indexOff+f.indexLen])
	if err != nil {
		t.Fatalf("decodeIndexBlock: %v", err)
	}
	types := make([]byte, len(index))
	for i, e := range index {
		types[i] = data[e.offset+uint64(e.length)-5]
	}
	return types
}

func TestCompressionShrinksTable(t *testing.T) {
	entries := compressibleEntries(500)
	compressed := buildTable(t, Options{}, entries)
	raw := buildTable(t, Options{DisableCompression: true}, entries)

	if len(compressed) >= len(raw) {
		t.Fatalf("compressed table (%d bytes) not smaller than raw (%d bytes)", len(compressed), len(raw))
	}
	for _, typ := range blockTypes(t, compressed) {
		if typ != blockTypeS2 {
			t.Fatalf("block type = %d, want %d (s2)", typ, blockTypeS2)
		}
	}
	// The compressed table still round-trips.
	r := openTable(t, compressed)
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v)", e.key, ok, err)
		}
	}
}

func TestIncompressibleBlocksStoredRaw(t *testing.T) {
	entries := incompressibleEntries(200)
	data := buildTable(t, Options{}, entries)

	for _, typ := range blockTypes(t, data) {
		if typ != blockTypeRaw {
			t.Fatalf("block type = %d, want %d (raw fallback)", typ, blockTypeRaw)
		}
	}
	r := openTable(t, data)
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v)", e.key, ok, err)
		}
	}
}

func TestDisableCompressionRoundTrip(t *testing.T) {
	entries := compressibleEntries(300)
	data := buildTable(t, Options{BlockSize: 512, DisableCompression: true}, entries)

	for _, typ := range blockTypes(t, data) {
		if typ != blockTypeRaw {
			t.Fatalf("block type = %d, want %d (raw)", typ, blockTypeRaw)
		}
	}
	r := openTable(t, data)
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v)", e.key, ok, err)
		}
	}
}

func TestCorruptCompressedBlockDetected(t *testing.T) {
	entries := compressibleEntries(500)
	data := buildTable(t, Options{BlockSize: 256}, entries)
	data[10] ^= 0xFF // inside the first (compressed) data block

	r := openTable(t, data)
	if _, _, err := r.Get(entries[0].key); err == nil {
		t.Fatal("Get on corrupted compressed block: want error")
	}
}

func TestUnknownBlockTypeRejected(t *testing.T) {
	data := buildTable(t, Options{DisableCompression: true}, compressibleEntries(10))
	f, err := decodeFooter(data[len(data)-footerSize:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	index, _, err := decodeIndexBlock(data[f.indexOff : f.indexOff+f.indexLen])
	if err != nil {
		t.Fatalf("decodeIndexBlock: %v", err)
	}
	// Overwrite the first block's type byte and re-seal its CRC so only the
	// type check can reject it.
	e := index[0]
	end := e.offset + uint64(e.length)
	data[end-5] = 0x7F
	binary.BigEndian.PutUint32(data[end-4:end], crc32.ChecksumIEEE(data[e.offset:end-4]))

	r := openTable(t, data)
	if _, _, err := r.Get([]byte("key-00000")); err == nil {
		t.Fatal("Get on unknown block type: want error")
	}
}

// assembleTable wraps the given verbatim on-disk block bytes in an otherwise
// valid version-2 table for the single key "a", so tests can hand a Reader a
// data block the Writer could never produce.
func assembleTable(block []byte) []byte {
	buf := append([]byte(nil), block...)
	index := []indexEntry{{firstKey: []byte("a"), offset: 0, length: uint32(len(block))}}
	f := footer{indexOff: uint64(len(buf)), count: 1, version: version2}
	ib := encodeIndexBlock(index, []byte("a"))
	buf = append(buf, ib...)
	f.indexLen = uint64(len(ib))
	f.bloomOff = uint64(len(buf))
	bl := newBloom(1, DefaultBitsPerKey)
	h1, h2 := murmur3.Sum128([]byte("a"))
	bl.addHash(h1, h2)
	bb := encodeBloomBlock(bl)
	buf = append(buf, bb...)
	f.bloomLen = uint64(len(bb))
	return append(buf, encodeFooter(f)...)
}

// buildBadBlockTable CRC-seals the given block content (payload + type byte)
// and assembles it into a table, so only post-CRC decode logic can reject it.
func buildBadBlockTable(block []byte) []byte {
	return assembleTable(binary.BigEndian.AppendUint32(append([]byte(nil), block...), crc32.ChecksumIEEE(block)))
}

func TestInvalidS2PayloadRejected(t *testing.T) {
	// 0xFF... is not a valid s2 stream: the CRC passes, decompression fails.
	block := append(bytes.Repeat([]byte{0xFF}, 8), blockTypeS2)
	r := openTable(t, buildBadBlockTable(block))
	if _, _, err := r.Get([]byte("a")); err == nil {
		t.Fatal("Get on undecodable s2 block: want error")
	}
}

func TestEmptyV2BlockBodyRejected(t *testing.T) {
	// A v2 block must hold at least its type byte; hand-build one that is
	// only a CRC trailer (the writer can never produce this).
	r := openTable(t, buildBadBlockTable(nil))
	if _, _, err := r.Get([]byte("a")); err == nil {
		t.Fatal("Get on typeless v2 block: want error")
	}
}

func TestUnsupportedFooterVersionRejected(t *testing.T) {
	data := buildTable(t, Options{}, sortedEntries(10))
	// Stamp version 3 into the footer and re-seal its CRC so only the
	// version check can reject it.
	off := len(data) - footerSize
	binary.BigEndian.PutUint16(data[off+40:], 3)
	binary.BigEndian.PutUint32(data[off+42:], crc32.ChecksumIEEE(data[off:off+42]))
	if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("NewReader on footer version 3: want error")
	}
}

// buildV1Table writes a table in the version-1 format — raw data blocks with
// no type byte — exactly as the pre-compression writer produced, so reader
// compatibility with tables already on disk stays pinned.
func buildV1Table(entries []kv, blockSize int) []byte {
	var buf []byte
	var index []indexEntry
	bl := newBloom(len(entries), DefaultBitsPerKey)
	var block, blockFirst []byte
	flush := func() {
		if len(block) == 0 {
			return
		}
		e := indexEntry{firstKey: blockFirst, offset: uint64(len(buf)), length: uint32(len(block) + 4)}
		block = binary.BigEndian.AppendUint32(block, crc32.ChecksumIEEE(block))
		buf = append(buf, block...)
		index = append(index, e)
		block, blockFirst = nil, nil
	}
	for _, e := range entries {
		if blockFirst == nil {
			blockFirst = e.key
		}
		block = appendEntry(block, e.key, e.value)
		h1, h2 := murmur3.Sum128(e.key)
		bl.addHash(h1, h2)
		if len(block) >= blockSize {
			flush()
		}
	}
	flush()
	f := footer{indexOff: uint64(len(buf)), count: uint64(len(entries)), version: version1}
	var largest []byte
	if len(entries) > 0 {
		largest = entries[len(entries)-1].key
	}
	ib := encodeIndexBlock(index, largest)
	buf = append(buf, ib...)
	f.indexLen = uint64(len(ib))
	f.bloomOff = uint64(len(buf))
	bb := encodeBloomBlock(bl)
	buf = append(buf, bb...)
	f.bloomLen = uint64(len(bb))
	return append(buf, encodeFooter(f)...)
}

func TestReadsVersion1Table(t *testing.T) {
	entries := sortedEntries(500)
	data := buildV1Table(entries, 256)
	r := openTable(t, data)

	if r.Count() != 500 {
		t.Fatalf("Count = %d, want 500", r.Count())
	}
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v)", e.key, ok, err)
		}
	}
	it := r.Iter()
	i := 0
	for it.Next() {
		assertEntryAt(t, it, entries, i)
		i++
	}
	if err := it.Err(); err != nil || i != len(entries) {
		t.Fatalf("iterated %d entries, err=%v", i, err)
	}
}

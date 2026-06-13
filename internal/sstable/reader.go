package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// Reader serves point lookups and ordered scans from a finished table. The
// block index and bloom filter live in memory; data blocks are read on
// demand, one ReadAt per lookup. Safe for concurrent use.
type Reader struct {
	r        io.ReaderAt
	index    []indexEntry
	bloom    *bloom
	count    uint64
	smallest []byte
	largest  []byte
	closer   io.Closer
}

// Open opens the table file at path. The returned Reader owns the file
// handle; call Close when done.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: stat: %w", err)
	}
	r, err := NewReader(f, st.Size())
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r.closer = f
	return r, nil
}

// NewReader opens a table from any io.ReaderAt of the given size, verifying
// the footer, index, and bloom filter CRCs up front. Data block CRCs are
// verified on each read.
func NewReader(ra io.ReaderAt, size int64) (*Reader, error) {
	if size < footerSize {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", errBadFooter, size)
	}
	fb := make([]byte, footerSize)
	if _, err := ra.ReadAt(fb, size-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	f, err := decodeFooter(fb)
	if err != nil {
		return nil, err
	}
	fsize := uint64(size)
	if f.indexOff > fsize || f.indexLen > fsize-f.indexOff ||
		f.bloomOff > fsize || f.bloomLen > fsize-f.bloomOff {
		return nil, fmt.Errorf("%w: block extents exceed file size", errBadFooter)
	}
	indexBuf := make([]byte, f.indexLen)
	if _, err := ra.ReadAt(indexBuf, int64(f.indexOff)); err != nil {
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	index, largest, err := decodeIndexBlock(indexBuf)
	if err != nil {
		return nil, err
	}
	for _, e := range index {
		if e.offset > f.indexOff || uint64(e.length) > f.indexOff-e.offset {
			return nil, fmt.Errorf("%w: data block extent exceeds index offset", errBadFooter)
		}
	}
	bloomBuf := make([]byte, f.bloomLen)
	if _, err := ra.ReadAt(bloomBuf, int64(f.bloomOff)); err != nil {
		return nil, fmt.Errorf("sstable: read bloom: %w", err)
	}
	bl, err := decodeBloomBlock(bloomBuf)
	if err != nil {
		return nil, err
	}
	r := &Reader{r: ra, index: index, bloom: bl, count: f.count}
	if len(index) > 0 {
		r.smallest = index[0].firstKey
		r.largest = largest
	}
	return r, nil
}

// Get returns the value stored for key. The returned slice does not alias
// reader-internal state and is safe to retain.
func (r *Reader) Get(key []byte) ([]byte, bool, error) {
	if r.count == 0 || !r.bloom.mayContain(key) {
		return nil, false, nil
	}
	// Find the last block whose first key is <= key; only it can hold key.
	i := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].firstKey, key) > 0
	}) - 1
	if i < 0 {
		return nil, false, nil
	}
	block, err := r.readBlock(r.index[i])
	if err != nil {
		return nil, false, err
	}
	for pos := 0; pos < len(block); {
		k, v, n, err := decodeEntry(block[pos:])
		if err != nil {
			return nil, false, err
		}
		switch bytes.Compare(k, key) {
		case 0:
			return v, true, nil
		case 1: // entries are sorted; key is not in this block
			return nil, false, nil
		}
		pos += n
	}
	return nil, false, nil
}

// Count returns the number of entries in the table.
func (r *Reader) Count() uint64 { return r.count }

// Smallest returns the table's first key, or nil for an empty table.
func (r *Reader) Smallest() []byte { return r.smallest }

// Largest returns the table's last key, or nil for an empty table.
func (r *Reader) Largest() []byte { return r.largest }

// Close releases the underlying file handle when the Reader was created via
// Open; otherwise it is a no-op.
func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

func (r *Reader) readBlock(e indexEntry) ([]byte, error) {
	if e.length < 4 {
		return nil, errors.New("sstable: invalid block length")
	}
	buf := make([]byte, e.length)
	if _, err := r.r.ReadAt(buf, int64(e.offset)); err != nil {
		return nil, fmt.Errorf("sstable: read block: %w", err)
	}
	body := buf[:len(buf)-4]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(buf[len(buf)-4:]) {
		return nil, fmt.Errorf("%w: data block at offset %d", errCRC, e.offset)
	}
	return body, nil
}

// Iter returns an iterator over all entries in key order, positioned before
// the first entry. Not safe for concurrent use, but independent iterators
// from the same Reader are.
func (r *Reader) Iter() *Iterator {
	return &Iterator{r: r}
}

// Iterator walks a table's entries in key order:
//
//	it := r.Iter()
//	for it.Next() {
//	    use(it.Key(), it.Value())
//	}
//	if err := it.Err(); err != nil { ... }
//
// Key and Value remain valid only until the next call to Next.
type Iterator struct {
	r        *Reader
	blockIdx int
	block    []byte
	pos      int
	key, val []byte
	err      error
}

// Next advances to the next entry. It returns false at the end of the table
// or on error; check Err to distinguish.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	for {
		if it.block == nil {
			if it.blockIdx >= len(it.r.index) {
				return false
			}
			b, err := it.r.readBlock(it.r.index[it.blockIdx])
			if err != nil {
				it.err = err
				return false
			}
			it.block = b
			it.pos = 0
		}
		if it.pos >= len(it.block) {
			it.block = nil
			it.blockIdx++
			continue
		}
		k, v, n, err := decodeEntry(it.block[it.pos:])
		if err != nil {
			it.err = err
			return false
		}
		it.pos += n
		it.key, it.val = k, v
		return true
	}
}

// Key returns the current entry's key. Valid until the next call to Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current entry's value. Valid until the next call to Next.
func (it *Iterator) Value() []byte { return it.val }

// Err returns the first error encountered during iteration, if any.
func (it *Iterator) Err() error { return it.err }

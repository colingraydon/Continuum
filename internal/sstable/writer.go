package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/twmb/murmur3"
)

// Writer builds an SSTable by streaming sorted entries to w. Keys must be
// added in strictly increasing byte order. The caller owns durability:
// write to a temp file, Finish, fsync, then rename into place.
//
// Not safe for concurrent use.
type Writer struct {
	w          io.Writer
	opts       Options
	offset     uint64
	block      []byte
	cbuf       []byte // reusable s2 output buffer
	blockFirst []byte
	index      []indexEntry
	hashes     []bloomHash
	lastKey    []byte
	count      uint64
	finished   bool
}

// bloomHash buffers a key's murmur3-128 halves so the filter can be sized
// exactly at Finish, when the key count is known.
type bloomHash struct{ h1, h2 uint64 }

// NewWriter returns a Writer that streams a table to w. A zero Options
// selects DefaultBlockSize and DefaultBitsPerKey.
func NewWriter(w io.Writer, opts Options) *Writer {
	return &Writer{w: w, opts: opts.withDefaults()}
}

// Add appends one entry. Keys must be non-empty and strictly greater than
// the previously added key.
func (w *Writer) Add(key, value []byte) error {
	if w.finished {
		return errors.New("sstable: add after finish")
	}
	if len(key) == 0 {
		return errors.New("sstable: empty key")
	}
	if len(key) > maxKeyLen {
		return fmt.Errorf("sstable: key too long: %d bytes", len(key))
	}
	if len(value) > maxValueLen {
		return fmt.Errorf("sstable: value too long: %d bytes", len(value))
	}
	if w.lastKey != nil && bytes.Compare(key, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: keys must be strictly increasing: %q after %q", key, w.lastKey)
	}
	if w.blockFirst == nil {
		w.blockFirst = append([]byte(nil), key...)
	}
	w.block = appendEntry(w.block, key, value)
	h1, h2 := murmur3.Sum128(key)
	w.hashes = append(w.hashes, bloomHash{h1, h2})
	w.lastKey = append(w.lastKey[:0], key...)
	w.count++
	if len(w.block) >= w.opts.BlockSize {
		return w.flushBlock()
	}
	return nil
}

// Finish flushes the final data block and writes the index, bloom filter,
// and footer. The Writer cannot be used afterwards. Finishing an empty
// table is valid and produces a table with zero entries.
func (w *Writer) Finish() error {
	if w.finished {
		return errors.New("sstable: finish called twice")
	}
	w.finished = true
	if len(w.block) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	f := footer{indexOff: w.offset, count: w.count, version: version2}
	indexBlock := encodeIndexBlock(w.index, w.lastKey)
	if err := w.write(indexBlock); err != nil {
		return err
	}
	f.indexLen = uint64(len(indexBlock))
	f.bloomOff = w.offset
	bl := newBloom(len(w.hashes), w.opts.BitsPerKey)
	for _, h := range w.hashes {
		bl.addHash(h.h1, h.h2)
	}
	bloomBlock := encodeBloomBlock(bl)
	if err := w.write(bloomBlock); err != nil {
		return err
	}
	f.bloomLen = uint64(len(bloomBlock))
	return w.write(encodeFooter(f))
}

// flushBlock writes the buffered block as payload | type:1 | crc32:4. The
// payload is s2-compressed unless compression is disabled or does not shrink
// this block, in which case it is stored raw.
func (w *Writer) flushBlock() error {
	body, typ := w.block, blockTypeRaw
	if !w.opts.DisableCompression {
		w.cbuf = s2.Encode(w.cbuf[:0], w.block)
		if len(w.cbuf) < len(w.block) {
			body, typ = w.cbuf, blockTypeS2
		}
	}
	body = append(body, typ)
	body = binary.BigEndian.AppendUint32(body, crc32.ChecksumIEEE(body))
	e := indexEntry{
		firstKey: w.blockFirst,
		offset:   w.offset,
		length:   uint32(len(body)),
	}
	if err := w.write(body); err != nil {
		return err
	}
	w.index = append(w.index, e)
	w.block = w.block[:0]
	w.blockFirst = nil
	return nil
}

func (w *Writer) write(b []byte) error {
	if _, err := w.w.Write(b); err != nil {
		return fmt.Errorf("sstable: write: %w", err)
	}
	w.offset += uint64(len(b))
	return nil
}

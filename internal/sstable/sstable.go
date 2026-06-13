// Package sstable implements immutable sorted string tables: the on-disk
// read layer of an LSM tree. A table holds key-sorted entries in CRC-checked
// data blocks, followed by a block index, a bloom filter, and a fixed-size
// footer. Values are opaque byte payloads; the encoding of value contents is
// the caller's responsibility, mirroring internal/wal.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk layout:
//
//   [data block]... [index block] [bloom block] [footer]
//
// data block:   (key_len:2 | key | val_len:4 | val)... | crc32:4
//   Entries are sorted by key. A block closes at the first entry that pushes
//   it past BlockSize, so a single oversized entry gets its own block.
//
// index block:  count:4 | (first_key_len:2 | first_key | offset:8 | len:4)×count
//               | largest_key_len:2 | largest_key | crc32:4
//   offset/len locate each data block; len includes the CRC trailer.
//
// bloom block:  k:1 | bits | crc32:4
//
// footer (50 bytes, fixed):
//   index_off:8 | index_len:8 | bloom_off:8 | bloom_len:8 | entry_count:8 |
//   version:2 | crc32:4 | magic:4 ("CSST")
//   CRC covers the 42 bytes before it. Readers locate the footer from the end
//   of the file, so tables are self-describing.

const (
	// DefaultBlockSize is the target uncompressed size of a data block.
	DefaultBlockSize = 4 * 1024
	// DefaultBitsPerKey sizes the bloom filter at ~1% false positives.
	DefaultBitsPerKey = 10

	maxKeyLen   = 1<<16 - 1
	maxValueLen = 1 << 30

	footerSize = 50
	version    = uint16(1)
	magic      = "CSST"
)

var (
	errCRC            = errors.New("sstable: crc mismatch")
	errBadFooter      = errors.New("sstable: bad footer")
	errTruncatedEntry = errors.New("sstable: truncated entry")
	errTruncatedIndex = errors.New("sstable: truncated index block")
)

// Options configure table construction. The zero value selects defaults.
type Options struct {
	// BlockSize is the target uncompressed size of a data block in bytes.
	BlockSize int
	// BitsPerKey sizes the bloom filter; 10 gives ~1% false positives.
	BitsPerKey int
}

func (o Options) withDefaults() Options {
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
	if o.BitsPerKey <= 0 {
		o.BitsPerKey = DefaultBitsPerKey
	}
	return o
}

// indexEntry locates one data block and carries its first key for binary
// search.
type indexEntry struct {
	firstKey []byte
	offset   uint64
	length   uint32
}

func appendEntry(buf, key, value []byte) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(value)))
	return append(buf, value...)
}

// decodeEntry reads one entry from the front of b, returning the key, value,
// and total encoded size. key and value alias b.
func decodeEntry(b []byte) (key, value []byte, n int, err error) {
	if len(b) < 2 {
		return nil, nil, 0, errTruncatedEntry
	}
	kl := int(binary.BigEndian.Uint16(b))
	if len(b) < 2+kl+4 {
		return nil, nil, 0, errTruncatedEntry
	}
	vl := int(binary.BigEndian.Uint32(b[2+kl:]))
	n = 2 + kl + 4 + vl
	if len(b) < n {
		return nil, nil, 0, errTruncatedEntry
	}
	return b[2 : 2+kl], b[2+kl+4 : n], n, nil
}

type footer struct {
	indexOff, indexLen uint64
	bloomOff, bloomLen uint64
	count              uint64
}

func encodeFooter(f footer) []byte {
	buf := make([]byte, 0, footerSize)
	buf = binary.BigEndian.AppendUint64(buf, f.indexOff)
	buf = binary.BigEndian.AppendUint64(buf, f.indexLen)
	buf = binary.BigEndian.AppendUint64(buf, f.bloomOff)
	buf = binary.BigEndian.AppendUint64(buf, f.bloomLen)
	buf = binary.BigEndian.AppendUint64(buf, f.count)
	buf = binary.BigEndian.AppendUint16(buf, version)
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
	return append(buf, magic...)
}

func decodeFooter(b []byte) (footer, error) {
	if len(b) != footerSize {
		return footer{}, errBadFooter
	}
	if string(b[46:50]) != magic {
		return footer{}, fmt.Errorf("%w: bad magic %q", errBadFooter, b[46:50])
	}
	if crc32.ChecksumIEEE(b[:42]) != binary.BigEndian.Uint32(b[42:46]) {
		return footer{}, fmt.Errorf("%w: crc mismatch", errBadFooter)
	}
	if v := binary.BigEndian.Uint16(b[40:42]); v != version {
		return footer{}, fmt.Errorf("sstable: unsupported version %d", v)
	}
	return footer{
		indexOff: binary.BigEndian.Uint64(b[0:8]),
		indexLen: binary.BigEndian.Uint64(b[8:16]),
		bloomOff: binary.BigEndian.Uint64(b[16:24]),
		bloomLen: binary.BigEndian.Uint64(b[24:32]),
		count:    binary.BigEndian.Uint64(b[32:40]),
	}, nil
}

func encodeIndexBlock(index []indexEntry, largest []byte) []byte {
	buf := binary.BigEndian.AppendUint32(nil, uint32(len(index)))
	for _, e := range index {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(e.firstKey)))
		buf = append(buf, e.firstKey...)
		buf = binary.BigEndian.AppendUint64(buf, e.offset)
		buf = binary.BigEndian.AppendUint32(buf, e.length)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(largest)))
	buf = append(buf, largest...)
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

func decodeIndexBlock(b []byte) ([]indexEntry, []byte, error) {
	if len(b) < 10 { // count:4 | largest_len:2 | crc:4
		return nil, nil, errTruncatedIndex
	}
	body := b[:len(b)-4]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, nil, fmt.Errorf("%w: index block", errCRC)
	}
	count := int(binary.BigEndian.Uint32(body))
	// Each index entry is at least 15 bytes (2+1 key, 8 offset, 4 length);
	// reject implausible counts before allocating.
	if count > len(body)/15 {
		return nil, nil, errTruncatedIndex
	}
	index := make([]indexEntry, 0, count)
	pos := 4
	for i := 0; i < count; i++ {
		if len(body) < pos+2 {
			return nil, nil, errTruncatedIndex
		}
		kl := int(binary.BigEndian.Uint16(body[pos:]))
		pos += 2
		if len(body) < pos+kl+12 {
			return nil, nil, errTruncatedIndex
		}
		e := indexEntry{firstKey: body[pos : pos+kl]}
		pos += kl
		e.offset = binary.BigEndian.Uint64(body[pos:])
		e.length = binary.BigEndian.Uint32(body[pos+8:])
		pos += 12
		index = append(index, e)
	}
	if len(body) < pos+2 {
		return nil, nil, errTruncatedIndex
	}
	ll := int(binary.BigEndian.Uint16(body[pos:]))
	pos += 2
	if len(body) < pos+ll {
		return nil, nil, errTruncatedIndex
	}
	return index, body[pos : pos+ll], nil
}

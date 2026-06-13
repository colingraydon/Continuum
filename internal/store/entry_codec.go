package store

import (
	"bytes"
	"fmt"
	"time"
)

// SSTable value encoding. The sstable package stores opaque values; this is
// what the store puts in them. First byte is the kind tag:
//
//   ENTRY (0x00): sib_count(2) | siblings | has_tomb(1) | tomb_at_ns(8)?
//     sibling:    value_len(4) | value | deleted(1) | clk_count(2) |
//                 (id_len(2) | id | counter(8))×clk_count
//   EVICT (0x01): nothing — the key was migrated away; shadows older tables
//
// The sibling layout matches the legacy snapshot body so writeSibling /
// readSibling are shared. tomb_at rides along for compaction-time GC.

const (
	tableKindEntry byte = 0x00
	tableKindEvict byte = 0x01
)

// tableEvictValue is the encoded value for an evict marker.
var tableEvictValue = []byte{tableKindEvict}

func encodeTableEntry(key string, e Entry, ages map[string]time.Time) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(tableKindEntry)
	if len(e.Siblings) > 0xFFFF {
		return nil, fmt.Errorf("store: too many siblings for %q: %d", key, len(e.Siblings))
	}
	_ = writeUint16(&buf, uint16(len(e.Siblings))) // bytes.Buffer.Write never fails
	for _, sib := range e.Siblings {
		if err := writeSibling(&buf, key, sib); err != nil {
			return nil, err
		}
	}
	if err := writeTombstoneAge(&buf, key, ages); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeTableEntry decodes an SSTable value. evicted=true means the value is
// an evict marker and the key should be treated as absent. The tombstone age
// is parsed but discarded; callers that need it use decodeTableEntryFull.
func decodeTableEntry(val []byte) (e Entry, evicted bool, err error) {
	e, evicted, _, _, err = decodeTableEntryFull(val)
	return e, evicted, err
}

// decodeTableEntryFull decodes an SSTable value, additionally returning the
// tombstone age. tombAt is meaningful only when hasTomb is true. Compaction
// uses the age to drop tombstones that have outlived the GC window.
func decodeTableEntryFull(val []byte) (e Entry, evicted bool, tombAt time.Time, hasTomb bool, err error) {
	if len(val) < 1 {
		return Entry{}, false, time.Time{}, false, fmt.Errorf("%w: empty value", errBadTableEntry)
	}
	switch val[0] {
	case tableKindEvict:
		return Entry{}, true, time.Time{}, false, nil
	case tableKindEntry:
	default:
		return Entry{}, false, time.Time{}, false, fmt.Errorf("%w: unknown kind 0x%02x", errBadTableEntry, val[0])
	}
	r := bytes.NewReader(val[1:])
	sibCount, err := readUint16(r)
	if err != nil {
		return Entry{}, false, time.Time{}, false, fmt.Errorf("%w: sib count: %v", errBadTableEntry, err)
	}
	sibs := make([]Sibling, sibCount)
	for i := range sibs {
		sib, err := readSibling(r, i)
		if err != nil {
			return Entry{}, false, time.Time{}, false, fmt.Errorf("%w: %v", errBadTableEntry, err)
		}
		sibs[i] = sib
	}
	tombAt, hasTomb, err = readTombstoneAge(r)
	if err != nil {
		return Entry{}, false, time.Time{}, false, fmt.Errorf("%w: %v", errBadTableEntry, err)
	}
	return Entry{Siblings: sibs}, false, tombAt, hasTomb, nil
}

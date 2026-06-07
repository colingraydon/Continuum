package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)

// WAL record type tags. First byte of every WAL payload.
const (
	recPut        byte = 0x01
	recDelete     byte = 0x02
	recEvict      byte = 0x03
	recGC         byte = 0x04
	recCheckpoint byte = 0x05
)

// Record layouts (after the leading type byte):
//
//   PUT:        key_len(2) | key | value_len(4) | value | n(2) | (id_len(2) | id | counter(8))×n
//   DELETE:     key_len(2) | key | tombstone_at_ns(8) | n(2) | (id_len(2) | id | counter(8))×n
//   EVICT:      key_len(2) | key
//   GC:         n(4) | (key_len(2) | key)×n
//   CHECKPOINT: snapshot_seq(8)
//
// All integer fields are big-endian. Clock entries are sorted by id for
// deterministic encoding.

func sortedClockIDs(c map[string]uint64) []string {
	ids := make([]string, 0, len(c))
	for id := range c {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func writeClocks(buf *bytes.Buffer, clocks map[string]uint64) error {
	if len(clocks) > 0xFFFF {
		return fmt.Errorf("record: too many clock entries: %d", len(clocks))
	}
	_ = writeUint16(buf, uint16(len(clocks))) // bytes.Buffer.Write never fails
	for _, id := range sortedClockIDs(clocks) {
		if err := writeLenString16(buf, id); err != nil {
			return err
		}
		_ = writeUint64(buf, clocks[id]) // bytes.Buffer.Write never fails
	}
	return nil
}

func readClocks(r io.Reader) (map[string]uint64, error) {
	n, err := readUint16(r)
	if err != nil {
		return nil, fmt.Errorf("clock count: %w", err)
	}
	clocks := make(map[string]uint64, n)
	for i := uint16(0); i < n; i++ {
		id, err := readLenString16(r)
		if err != nil {
			return nil, fmt.Errorf("clock %d id: %w", i, err)
		}
		ctr, err := readUint64(r)
		if err != nil {
			return nil, fmt.Errorf("clock %d counter: %w", i, err)
		}
		clocks[id] = ctr
	}
	return clocks, nil
}

func encodePut(key, value string, v VectorClockVersion) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recPut)
	if err := writeLenString16(&buf, key); err != nil {
		return nil, err
	}
	_ = writeLenString32(&buf, value) // bytes.Buffer.Write never fails
	if err := writeClocks(&buf, v.Clocks); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePut(body []byte) (string, string, VectorClockVersion, error) {
	r := bytes.NewReader(body)
	key, err := readLenString16(r)
	if err != nil {
		return "", "", VectorClockVersion{}, fmt.Errorf("put key: %w", err)
	}
	value, err := readLenString32(r)
	if err != nil {
		return "", "", VectorClockVersion{}, fmt.Errorf("put value: %w", err)
	}
	clocks, err := readClocks(r)
	if err != nil {
		return "", "", VectorClockVersion{}, fmt.Errorf("put clocks: %w", err)
	}
	return key, value, VectorClockVersion{Clocks: clocks}, nil
}

func encodeDelete(key string, tombAt time.Time, v VectorClockVersion) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recDelete)
	if err := writeLenString16(&buf, key); err != nil {
		return nil, err
	}
	_ = writeUint64(&buf, uint64(tombAt.UnixNano())) // bytes.Buffer.Write never fails
	if err := writeClocks(&buf, v.Clocks); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeDelete(body []byte) (string, time.Time, VectorClockVersion, error) {
	r := bytes.NewReader(body)
	key, err := readLenString16(r)
	if err != nil {
		return "", time.Time{}, VectorClockVersion{}, fmt.Errorf("delete key: %w", err)
	}
	ts, err := readUint64(r)
	if err != nil {
		return "", time.Time{}, VectorClockVersion{}, fmt.Errorf("delete ts: %w", err)
	}
	clocks, err := readClocks(r)
	if err != nil {
		return "", time.Time{}, VectorClockVersion{}, fmt.Errorf("delete clocks: %w", err)
	}
	return key, time.Unix(0, int64(ts)), VectorClockVersion{Clocks: clocks}, nil
}

func encodeEvict(key string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(recEvict)
	if err := writeLenString16(&buf, key); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeEvict(body []byte) (string, error) {
	r := bytes.NewReader(body)
	key, err := readLenString16(r)
	if err != nil {
		return "", fmt.Errorf("evict key: %w", err)
	}
	return key, nil
}

func encodeGC(keys []string) ([]byte, error) {
	if uint64(len(keys)) > 0xFFFFFFFF {
		return nil, fmt.Errorf("record: too many gc keys: %d", len(keys))
	}
	var buf bytes.Buffer
	buf.WriteByte(recGC)
	var n [4]byte
	n[0] = byte(uint32(len(keys)) >> 24)
	n[1] = byte(uint32(len(keys)) >> 16)
	n[2] = byte(uint32(len(keys)) >> 8)
	n[3] = byte(uint32(len(keys)))
	buf.Write(n[:])
	for _, k := range keys {
		if err := writeLenString16(&buf, k); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeGC(body []byte) ([]string, error) {
	r := bytes.NewReader(body)
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, fmt.Errorf("gc count: %w", err)
	}
	count := uint32(n[0])<<24 | uint32(n[1])<<16 | uint32(n[2])<<8 | uint32(n[3])
	keys := make([]string, count)
	for i := range keys {
		k, err := readLenString16(r)
		if err != nil {
			return nil, fmt.Errorf("gc key %d: %w", i, err)
		}
		keys[i] = k
	}
	return keys, nil
}

func encodeCheckpoint(snapshotSeq uint64) []byte {
	var buf bytes.Buffer
	buf.WriteByte(recCheckpoint)
	_ = writeUint64(&buf, snapshotSeq) // bytes.Buffer.Write never fails
	return buf.Bytes()
}

func decodeCheckpoint(body []byte) (uint64, error) {
	r := bytes.NewReader(body)
	return readUint64(r)
}

// errUnknownRecordType is returned by applyWALRecord for an unrecognized
// type byte.
var errUnknownRecordType = errors.New("store: unknown wal record type")

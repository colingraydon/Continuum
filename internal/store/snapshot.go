package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"time"

	"github.com/spaolacci/murmur3"
)

// Snapshot file layout:
//
//   header:
//     magic[4]        "CONT"
//     version[2]      uint16
//     epoch[8]        uint64
//     sequence_at[8]  uint64 — WAL sequence captured by this snapshot
//     entry_count[8]  uint64
//     node_id_len[2]  uint16
//     node_id[N]      bytes
//     header_crc[4]   CRC32(magic | version | epoch | seq | count | id_len | node_id)
//
//   body: entry_count×{
//     key_len[2]      uint16
//     key[K]          bytes
//     sib_count[2]    uint16
//     siblings[]{
//       value_len[4]  uint32
//       value[V]      bytes
//       deleted[1]    uint8
//       clk_count[2]  uint16
//       clocks[]{ id_len[2] | id | counter[8] }
//     }
//     has_tombstone[1] uint8
//     tombstone_at[8]  int64 (UnixNano, omitted when has_tombstone==0)
//   }
//   body_crc[4]      CRC32 of the entire body, streamed
//
// Sibling.Hash is not persisted; it is recomputed from Value on load.

const (
	snapMagic   = "CONT"
	snapVersion = uint16(1)
)

// SnapHeader is the metadata captured at the top of a snapshot file.
type SnapHeader struct {
	NodeID     string
	Epoch      uint64
	SequenceAt uint64
	EntryCount uint64
}

// Snapshot serializes the store to w under header. The store mutex is held
// only long enough to copy the data and tombstone-age maps; the actual
// encoding happens outside the lock.
func (s *Store) Snapshot(w io.Writer, header SnapHeader) error {
	s.mu.RLock()
	data := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	ages := make(map[string]time.Time, len(s.tombstoneAges))
	for k, v := range s.tombstoneAges {
		ages[k] = v
	}
	s.mu.RUnlock()

	header.EntryCount = uint64(len(data))
	if err := writeSnapHeader(w, header); err != nil {
		return err
	}
	return writeSnapBody(w, data, ages)
}

// LoadSnapshot reads a snapshot from r and populates s, replacing any
// existing in-memory state. If expectedNodeID is non-empty and does not
// match the snapshot header's NodeID, the load is rejected.
func (s *Store) LoadSnapshot(r io.Reader, expectedNodeID string) (SnapHeader, error) {
	header, err := readSnapHeader(r)
	if err != nil {
		return SnapHeader{}, err
	}
	if expectedNodeID != "" && header.NodeID != expectedNodeID {
		return SnapHeader{}, fmt.Errorf("snapshot: node id mismatch: snapshot=%q expected=%q", header.NodeID, expectedNodeID)
	}
	data, ages, err := readSnapBody(r, header.EntryCount)
	if err != nil {
		return SnapHeader{}, err
	}
	s.mu.Lock()
	s.data = data
	s.tombstoneAges = ages
	s.mu.Unlock()
	return header, nil
}

func writeSnapHeader(w io.Writer, h SnapHeader) error {
	if len(h.NodeID) > 0xFFFF {
		return fmt.Errorf("snapshot: node id too long: %d", len(h.NodeID))
	}
	buf := make([]byte, 0, 32+len(h.NodeID)+4)
	buf = append(buf, []byte(snapMagic)...)
	buf = binary.BigEndian.AppendUint16(buf, snapVersion)
	buf = binary.BigEndian.AppendUint64(buf, h.Epoch)
	buf = binary.BigEndian.AppendUint64(buf, h.SequenceAt)
	buf = binary.BigEndian.AppendUint64(buf, h.EntryCount)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.NodeID)))
	buf = append(buf, []byte(h.NodeID)...)
	crc := crc32.ChecksumIEEE(buf)
	buf = binary.BigEndian.AppendUint32(buf, crc)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("snapshot: write header: %w", err)
	}
	return nil
}

func readSnapHeader(r io.Reader) (SnapHeader, error) {
	var prefix [32]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return SnapHeader{}, fmt.Errorf("snapshot: read header prefix: %w", err)
	}
	if string(prefix[0:4]) != snapMagic {
		return SnapHeader{}, fmt.Errorf("snapshot: bad magic %q", prefix[0:4])
	}
	ver := binary.BigEndian.Uint16(prefix[4:6])
	if ver != snapVersion {
		return SnapHeader{}, fmt.Errorf("snapshot: unsupported version %d", ver)
	}
	h := SnapHeader{
		Epoch:      binary.BigEndian.Uint64(prefix[6:14]),
		SequenceAt: binary.BigEndian.Uint64(prefix[14:22]),
		EntryCount: binary.BigEndian.Uint64(prefix[22:30]),
	}
	idLen := binary.BigEndian.Uint16(prefix[30:32])
	idBytes := make([]byte, idLen)
	if _, err := io.ReadFull(r, idBytes); err != nil {
		return SnapHeader{}, fmt.Errorf("snapshot: read node id: %w", err)
	}
	var crcBytes [4]byte
	if _, err := io.ReadFull(r, crcBytes[:]); err != nil {
		return SnapHeader{}, fmt.Errorf("snapshot: read header crc: %w", err)
	}
	want := binary.BigEndian.Uint32(crcBytes[:])
	got := crc32.Update(crc32.ChecksumIEEE(prefix[:]), crc32.IEEETable, idBytes)
	if got != want {
		return SnapHeader{}, errors.New("snapshot: header crc mismatch")
	}
	h.NodeID = string(idBytes)
	return h, nil
}

func writeSnapBody(w io.Writer, data map[string]Entry, ages map[string]time.Time) error {
	cw := &crcWriter{w: w}
	for key, entry := range data {
		if err := writeEntry(cw, key, entry, ages); err != nil {
			return err
		}
	}
	var crcBytes [4]byte
	binary.BigEndian.PutUint32(crcBytes[:], cw.crc)
	if _, err := w.Write(crcBytes[:]); err != nil {
		return fmt.Errorf("snapshot: write body crc: %w", err)
	}
	return nil
}

func readSnapBody(r io.Reader, n uint64) (map[string]Entry, map[string]time.Time, error) {
	cr := &crcReader{r: r}
	data := make(map[string]Entry, n)
	ages := make(map[string]time.Time)
	for i := uint64(0); i < n; i++ {
		key, entry, t, hasTomb, err := readEntry(cr)
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot: entry %d: %w", i, err)
		}
		data[key] = entry
		if hasTomb {
			ages[key] = t
		}
	}
	var crcBytes [4]byte
	if _, err := io.ReadFull(r, crcBytes[:]); err != nil {
		return nil, nil, fmt.Errorf("snapshot: read body crc: %w", err)
	}
	if cr.crc != binary.BigEndian.Uint32(crcBytes[:]) {
		return nil, nil, errors.New("snapshot: body crc mismatch")
	}
	return data, ages, nil
}

func writeEntry(w io.Writer, key string, entry Entry, ages map[string]time.Time) error {
	if err := writeLenString16(w, key); err != nil {
		return err
	}
	if len(entry.Siblings) > 0xFFFF {
		return fmt.Errorf("snapshot: too many siblings for %q: %d", key, len(entry.Siblings))
	}
	if err := writeUint16(w, uint16(len(entry.Siblings))); err != nil {
		return err
	}
	for _, sib := range entry.Siblings {
		if err := writeLenString32(w, sib.Value); err != nil {
			return err
		}
		var d byte
		if sib.Deleted {
			d = 1
		}
		if _, err := w.Write([]byte{d}); err != nil {
			return err
		}
		if len(sib.Version.Clocks) > 0xFFFF {
			return fmt.Errorf("snapshot: too many clock entries for %q: %d", key, len(sib.Version.Clocks))
		}
		if err := writeUint16(w, uint16(len(sib.Version.Clocks))); err != nil {
			return err
		}
		// Sort for deterministic encoding (helps tests and reproducibility).
		ids := make([]string, 0, len(sib.Version.Clocks))
		for id := range sib.Version.Clocks {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if err := writeLenString16(w, id); err != nil {
				return err
			}
			if err := writeUint64(w, sib.Version.Clocks[id]); err != nil {
				return err
			}
		}
	}
	if t, ok := ages[key]; ok {
		if _, err := w.Write([]byte{1}); err != nil {
			return err
		}
		if err := writeUint64(w, uint64(t.UnixNano())); err != nil {
			return err
		}
	} else {
		if _, err := w.Write([]byte{0}); err != nil {
			return err
		}
	}
	return nil
}

func readEntry(r io.Reader) (string, Entry, time.Time, bool, error) {
	key, err := readLenString16(r)
	if err != nil {
		return "", Entry{}, time.Time{}, false, fmt.Errorf("key: %w", err)
	}
	sibCount, err := readUint16(r)
	if err != nil {
		return "", Entry{}, time.Time{}, false, fmt.Errorf("sib count: %w", err)
	}
	sibs := make([]Sibling, sibCount)
	for i := range sibs {
		value, err := readLenString32(r)
		if err != nil {
			return "", Entry{}, time.Time{}, false, fmt.Errorf("sib %d value: %w", i, err)
		}
		var del [1]byte
		if _, err := io.ReadFull(r, del[:]); err != nil {
			return "", Entry{}, time.Time{}, false, fmt.Errorf("sib %d deleted: %w", i, err)
		}
		clkCount, err := readUint16(r)
		if err != nil {
			return "", Entry{}, time.Time{}, false, fmt.Errorf("sib %d clk count: %w", i, err)
		}
		clocks := make(map[string]uint64, clkCount)
		for j := uint16(0); j < clkCount; j++ {
			id, err := readLenString16(r)
			if err != nil {
				return "", Entry{}, time.Time{}, false, fmt.Errorf("sib %d clk id: %w", i, err)
			}
			counter, err := readUint64(r)
			if err != nil {
				return "", Entry{}, time.Time{}, false, fmt.Errorf("sib %d counter: %w", i, err)
			}
			clocks[id] = counter
		}
		sibs[i] = Sibling{
			Value:   value,
			Deleted: del[0] == 1,
			Version: VectorClockVersion{Clocks: clocks},
		}
		if !sibs[i].Deleted {
			sibs[i].Hash = murmur3.Sum32([]byte(value))
		}
	}
	var hasTomb [1]byte
	if _, err := io.ReadFull(r, hasTomb[:]); err != nil {
		return "", Entry{}, time.Time{}, false, fmt.Errorf("has tombstone: %w", err)
	}
	var t time.Time
	if hasTomb[0] == 1 {
		ts, err := readUint64(r)
		if err != nil {
			return "", Entry{}, time.Time{}, false, fmt.Errorf("tombstone ts: %w", err)
		}
		t = time.Unix(0, int64(ts))
	}
	return key, Entry{Siblings: sibs}, t, hasTomb[0] == 1, nil
}

// --- encoding helpers ---

type crcWriter struct {
	w   io.Writer
	crc uint32
}

func (c *crcWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	}
	return n, err
}

type crcReader struct {
	r   io.Reader
	crc uint32
}

func (c *crcReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	}
	return n, err
}

func writeUint16(w io.Writer, v uint16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeUint64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeLenString16(w io.Writer, s string) error {
	if len(s) > 0xFFFF {
		return fmt.Errorf("snapshot: string too long: %d", len(s))
	}
	if err := writeUint16(w, uint16(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func writeLenString32(w io.Writer, s string) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(s)))
	if _, err := w.Write(b[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func readLenString16(r io.Reader) (string, error) {
	n, err := readUint16(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readLenString32(r io.Reader) (string, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint32(b[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

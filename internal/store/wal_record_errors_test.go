package store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The encoders use bytes.Buffer internally, so the only way they can fail
// is when one of their fields hits a length cap (uint16). These tests
// confirm each cap is enforced.

func TestEncodePut_KeyTooLong(t *testing.T) {
	long := strings.Repeat("k", 1<<16)
	v := VectorClockVersion{Clocks: map[string]uint64{"a": 1}}
	if _, err := encodePut(long, "v", v); err == nil {
		t.Fatalf("expected error for key > 65535 bytes")
	}
}

func TestEncodePut_TooManyClocks(t *testing.T) {
	clocks := make(map[string]uint64, 1<<16+1)
	for i := 0; i <= 1<<16; i++ {
		clocks[fmt.Sprintf("n%d", i)] = 1
	}
	if _, err := encodePut("k", "v", VectorClockVersion{Clocks: clocks}); err == nil {
		t.Fatalf("expected error for >65535 clock entries")
	}
}

func TestEncodePut_ClockIDTooLong(t *testing.T) {
	clocks := map[string]uint64{strings.Repeat("x", 1<<16): 1}
	if _, err := encodePut("k", "v", VectorClockVersion{Clocks: clocks}); err == nil {
		t.Fatalf("expected error for clock id > 65535 bytes")
	}
}

func TestEncodeDelete_KeyTooLong(t *testing.T) {
	long := strings.Repeat("k", 1<<16)
	if _, err := encodeDelete(long, time.Now(), VectorClockVersion{}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestEncodeDelete_ClocksError(t *testing.T) {
	clocks := map[string]uint64{strings.Repeat("x", 1<<16): 1}
	if _, err := encodeDelete("k", time.Now(), VectorClockVersion{Clocks: clocks}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestEncodeEvict_KeyTooLong(t *testing.T) {
	if _, err := encodeEvict(strings.Repeat("k", 1<<16)); err == nil {
		t.Fatalf("expected error")
	}
}

func TestEncodeGC_KeyTooLong(t *testing.T) {
	if _, err := encodeGC([]string{strings.Repeat("k", 1<<16)}); err == nil {
		t.Fatalf("expected error")
	}
}

// Decode error paths: truncated payloads exercise readClocks and the decode
// functions called by applyWALRecord during Replay.

func TestReadClocks_CountTruncated(t *testing.T) {
	// readUint16 needs 2 bytes; provide only 1.
	r := strings.NewReader("\x00")
	if _, err := readClocks(r); err == nil {
		t.Fatal("expected error for truncated clock count")
	}
}

func TestReadClocks_IDTruncated(t *testing.T) {
	// count=1, then only 1 byte for the 2-byte id_len field.
	r := strings.NewReader("\x00\x01\x00")
	if _, err := readClocks(r); err == nil {
		t.Fatal("expected error for truncated clock id length")
	}
}

func TestReadClocks_CounterTruncated(t *testing.T) {
	// count=1, id_len=1, id='a', then only 3 bytes of the 8-byte counter.
	r := strings.NewReader("\x00\x01\x00\x01a\x00\x00\x00")
	if _, err := readClocks(r); err == nil {
		t.Fatal("expected error for truncated clock counter")
	}
}

func TestDecodePut_ValueTruncated(t *testing.T) {
	// key_len=1, key='k', value_len=100 but no value bytes.
	body := []byte{0x00, 0x01, 'k', 0x00, 0x00, 0x00, 0x64}
	if _, _, _, err := decodePut(body); err == nil {
		t.Fatal("expected error for truncated value")
	}
}

func TestDecodePut_ClocksTruncated(t *testing.T) {
	// key='k', value='v', then only 1 byte for the 2-byte clock count.
	body := []byte{0x00, 0x01, 'k', 0x00, 0x00, 0x00, 0x01, 'v', 0x00}
	if _, _, _, err := decodePut(body); err == nil {
		t.Fatal("expected error for truncated clocks in put")
	}
}

func TestDecodeDelete_TimestampTruncated(t *testing.T) {
	// key='k', then only 4 of the 8 timestamp bytes.
	body := []byte{0x00, 0x01, 'k', 0x00, 0x00, 0x00, 0x00}
	if _, _, _, err := decodeDelete(body); err == nil {
		t.Fatal("expected error for truncated timestamp")
	}
}

func TestDecodeDelete_ClocksTruncated(t *testing.T) {
	// key='k', 8-byte timestamp, then only 1 byte for the 2-byte clock count.
	body := []byte{0x00, 0x01, 'k', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if _, _, _, err := decodeDelete(body); err == nil {
		t.Fatal("expected error for truncated clocks in delete")
	}
}

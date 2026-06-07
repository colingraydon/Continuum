package store

import (
	"bytes"
	"testing"
	"time"
)

func TestEncodePut_RoundTrip(t *testing.T) {
	v := VectorClockVersion{Clocks: map[string]uint64{"a": 1, "b": 2}}
	enc, err := encodePut("key", "value", v)
	if err != nil {
		t.Fatalf("encodePut: %v", err)
	}
	if enc[0] != recPut {
		t.Fatalf("type byte = 0x%02x, want 0x%02x", enc[0], recPut)
	}
	k, val, gotV, err := decodePut(enc[1:])
	if err != nil {
		t.Fatalf("decodePut: %v", err)
	}
	if k != "key" || val != "value" {
		t.Fatalf("got (%q, %q), want (key, value)", k, val)
	}
	if !gotV.Equal(v) {
		t.Fatalf("clocks mismatch: %v vs %v", gotV, v)
	}
}

func TestEncodeDelete_RoundTrip(t *testing.T) {
	v := VectorClockVersion{Clocks: map[string]uint64{"a": 5}}
	now := time.Unix(0, 1234567890123)
	enc, err := encodeDelete("k", now, v)
	if err != nil {
		t.Fatalf("encodeDelete: %v", err)
	}
	if enc[0] != recDelete {
		t.Fatalf("type byte = 0x%02x", enc[0])
	}
	k, ts, gotV, err := decodeDelete(enc[1:])
	if err != nil {
		t.Fatalf("decodeDelete: %v", err)
	}
	if k != "k" {
		t.Fatalf("key = %q", k)
	}
	if !ts.Equal(now) {
		t.Fatalf("ts = %v, want %v", ts, now)
	}
	if !gotV.Equal(v) {
		t.Fatalf("clocks mismatch")
	}
}

func TestEncodeEvict_RoundTrip(t *testing.T) {
	enc, err := encodeEvict("evicted")
	if err != nil {
		t.Fatalf("encodeEvict: %v", err)
	}
	if enc[0] != recEvict {
		t.Fatalf("type byte = 0x%02x", enc[0])
	}
	k, err := decodeEvict(enc[1:])
	if err != nil {
		t.Fatalf("decodeEvict: %v", err)
	}
	if k != "evicted" {
		t.Fatalf("got %q", k)
	}
}

func TestEncodeGC_RoundTrip(t *testing.T) {
	keys := []string{"k1", "k2", "k3"}
	enc, err := encodeGC(keys)
	if err != nil {
		t.Fatalf("encodeGC: %v", err)
	}
	if enc[0] != recGC {
		t.Fatalf("type byte = 0x%02x", enc[0])
	}
	got, err := decodeGC(enc[1:])
	if err != nil {
		t.Fatalf("decodeGC: %v", err)
	}
	if len(got) != len(keys) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("got[%d] = %q, want %q", i, got[i], keys[i])
		}
	}
}

func TestEncodeGC_Empty(t *testing.T) {
	enc, err := encodeGC(nil)
	if err != nil {
		t.Fatalf("encodeGC: %v", err)
	}
	got, err := decodeGC(enc[1:])
	if err != nil {
		t.Fatalf("decodeGC: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestEncodeCheckpoint_RoundTrip(t *testing.T) {
	enc := encodeCheckpoint(99)
	if enc[0] != recCheckpoint {
		t.Fatalf("type byte = 0x%02x", enc[0])
	}
	got, err := decodeCheckpoint(enc[1:])
	if err != nil {
		t.Fatalf("decodeCheckpoint: %v", err)
	}
	if got != 99 {
		t.Fatalf("got %d, want 99", got)
	}
}

func TestEncodePut_DeterministicClockOrder(t *testing.T) {
	v := VectorClockVersion{Clocks: map[string]uint64{"z": 1, "a": 2, "m": 3}}
	first, err := encodePut("k", "v", v)
	if err != nil {
		t.Fatalf("encodePut: %v", err)
	}
	// Encode several more times — Go map iteration order is randomized but
	// our encoding sorts clock IDs, so the bytes must be identical.
	for i := 0; i < 20; i++ {
		again, err := encodePut("k", "v", v)
		if err != nil {
			t.Fatalf("encodePut: %v", err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("non-deterministic encoding")
		}
	}
}

func TestDecode_TruncatedReturnsError(t *testing.T) {
	cases := map[string]func([]byte) error{
		"put":        func(b []byte) error { _, _, _, err := decodePut(b); return err },
		"delete":     func(b []byte) error { _, _, _, err := decodeDelete(b); return err },
		"evict":      func(b []byte) error { _, err := decodeEvict(b); return err },
		"gc":         func(b []byte) error { _, err := decodeGC(b); return err },
		"checkpoint": func(b []byte) error { _, err := decodeCheckpoint(b); return err },
	}
	for name, fn := range cases {
		if err := fn(nil); err == nil {
			t.Fatalf("%s: expected error on empty body", name)
		}
	}
}

func TestDecodeGC_TruncatedKey(t *testing.T) {
	// Count=2 but only one key follows.
	body := []byte{0, 0, 0, 2, 0, 1, 'a'}
	if _, err := decodeGC(body); err == nil {
		t.Fatalf("expected error on truncated gc key list")
	}
}

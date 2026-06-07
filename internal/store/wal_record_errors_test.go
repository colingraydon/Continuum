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

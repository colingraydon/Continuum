package sstable

import (
	"fmt"
	"testing"

	"github.com/twmb/murmur3"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 10000
	b := newBloom(n, DefaultBitsPerKey)
	for i := 0; i < n; i++ {
		b.addHash(murmur3.Sum128(fmt.Appendf(nil, "key-%05d", i)))
	}
	for i := 0; i < n; i++ {
		if !b.mayContain(fmt.Appendf(nil, "key-%05d", i)) {
			t.Fatalf("false negative for key-%05d", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 10000
	b := newBloom(n, DefaultBitsPerKey)
	for i := 0; i < n; i++ {
		b.addHash(murmur3.Sum128(fmt.Appendf(nil, "key-%05d", i)))
	}
	falsePositives := 0
	for i := 0; i < n; i++ {
		if b.mayContain(fmt.Appendf(nil, "absent-%05d", i)) {
			falsePositives++
		}
	}
	// 10 bits/key with k=6 probes gives ~0.9% expected; 3% leaves headroom.
	if rate := float64(falsePositives) / n; rate > 0.03 {
		t.Fatalf("false positive rate %.4f exceeds 0.03", rate)
	}
}

func TestBloomKClamps(t *testing.T) {
	if k := bloomK(1); k != 1 {
		t.Fatalf("bloomK(1) = %d, want 1", k)
	}
	if k := bloomK(100); k != 30 {
		t.Fatalf("bloomK(100) = %d, want 30", k)
	}
	if k := bloomK(10); k != 6 {
		t.Fatalf("bloomK(10) = %d, want 6", k)
	}
}

func TestBloomRoundTrip(t *testing.T) {
	b := newBloom(100, DefaultBitsPerKey)
	for i := 0; i < 100; i++ {
		b.addHash(murmur3.Sum128(fmt.Appendf(nil, "key-%05d", i)))
	}
	decoded, err := decodeBloomBlock(encodeBloomBlock(b))
	if err != nil {
		t.Fatalf("decodeBloomBlock: %v", err)
	}
	if decoded.k != b.k || len(decoded.bits) != len(b.bits) {
		t.Fatalf("decoded k=%d bits=%d, want k=%d bits=%d", decoded.k, len(decoded.bits), b.k, len(b.bits))
	}
	for i := 0; i < 100; i++ {
		if !decoded.mayContain(fmt.Appendf(nil, "key-%05d", i)) {
			t.Fatalf("decoded filter lost key-%05d", i)
		}
	}
}

func TestBloomBlockCorruption(t *testing.T) {
	b := newBloom(100, DefaultBitsPerKey)
	block := encodeBloomBlock(b)
	block[1] ^= 0xFF
	if _, err := decodeBloomBlock(block); err == nil {
		t.Fatal("decodeBloomBlock with corrupted bits: want error")
	}
	if _, err := decodeBloomBlock(block[:3]); err == nil {
		t.Fatal("decodeBloomBlock on truncated block: want error")
	}
}

package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/twmb/murmur3"
)

// bloom is a standard Bloom filter using Kirsch-Mitzenmacher double hashing
// over murmur3's 128-bit output: bit_i = (h1 + i*h2) mod m. False negatives
// are impossible; the false-positive rate is set by bits-per-key (10 bits/key
// with k=6 probes gives ~1%).
type bloom struct {
	bits []byte
	k    byte
}

func newBloom(numKeys, bitsPerKey int) *bloom {
	nBits := numKeys * bitsPerKey
	if nBits < 64 {
		nBits = 64
	}
	return &bloom{bits: make([]byte, (nBits+7)/8), k: bloomK(bitsPerKey)}
}

// bloomK returns the optimal probe count for the given density,
// k = bitsPerKey * ln2, clamped to [1, 30].
func bloomK(bitsPerKey int) byte {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return byte(k)
}

func (b *bloom) addHash(h1, h2 uint64) {
	m := uint64(len(b.bits)) * 8
	for i := uint64(0); i < uint64(b.k); i++ {
		pos := (h1 + i*h2) % m
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

// mayContain reports whether key might be in the set. False means
// definitely absent.
func (b *bloom) mayContain(key []byte) bool {
	h1, h2 := murmur3.Sum128(key)
	m := uint64(len(b.bits)) * 8
	for i := uint64(0); i < uint64(b.k); i++ {
		pos := (h1 + i*h2) % m
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func encodeBloomBlock(b *bloom) []byte {
	buf := make([]byte, 0, 1+len(b.bits)+4)
	buf = append(buf, b.k)
	buf = append(buf, b.bits...)
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

func decodeBloomBlock(b []byte) (*bloom, error) {
	if len(b) < 5 { // k:1 | crc:4
		return nil, errors.New("sstable: bloom block too short")
	}
	body := b[:len(b)-4]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("%w: bloom block", errCRC)
	}
	return &bloom{k: body[0], bits: body[1:]}, nil
}

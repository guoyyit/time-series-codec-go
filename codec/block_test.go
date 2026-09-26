package codec

import (
	"math"
	"math/rand"
	"testing"
)

func TestBlockRoundTripLossless(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var samples []Sample
	v := 100.0
	for i := 0; i < 2000; i++ {
		v += rng.NormFloat64()
		samples = append(samples, Sample{T: int64(1700000000000 + i*1000), V: v})
	}
	data, err := encodeBlock(samples, ModeLossless, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(samples) {
		t.Fatalf("count mismatch: %d != %d", len(got), len(samples))
	}
	for i := range samples {
		if got[i].T != samples[i].T || math.Float64bits(got[i].V) != math.Float64bits(samples[i].V) {
			t.Fatalf("sample %d mismatch: got %+v want %+v", i, got[i], samples[i])
		}
	}
	t.Logf("lossless: %d samples, raw=%dB, encoded=%dB", len(samples), len(samples)*16, len(data))
}

func TestBlockRoundTripLossyBound(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const eps = 0.5
	var samples []Sample
	for i := 0; i < 1000; i++ {
		samples = append(samples, Sample{T: int64(i * 100), V: rng.Float64() * 1000})
	}
	data, err := encodeBlock(samples, ModeLossy, eps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range samples {
		if got[i].T != samples[i].T {
			t.Fatalf("timestamp %d mismatch", i)
		}
		if d := math.Abs(got[i].V - samples[i].V); d > eps {
			t.Fatalf("sample %d error %v exceeds epsilon %v", i, d, eps)
		}
	}
}

func TestBlockSingleAndConst(t *testing.T) {
	one := []Sample{{T: 5, V: 3.14}}
	data, err := encodeBlock(one, ModeLossless, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != one[0] {
		t.Fatalf("single sample mismatch: %+v", got)
	}

	var consts []Sample
	for i := 0; i < 100; i++ {
		consts = append(consts, Sample{T: int64(i), V: 1.5})
	}
	data, _ = encodeBlock(consts, ModeLossless, 0)
	got, _ = decodeBlock(data)
	for i := range consts {
		if got[i] != consts[i] {
			t.Fatalf("const sample %d mismatch", i)
		}
	}
}

func TestDecodeCorrupt(t *testing.T) {
	samples := []Sample{{T: 1, V: 1}, {T: 2, V: 2}}
	data, _ := encodeBlock(samples, ModeLossless, 0)
	data[len(data)-6] ^= 0xff // 破坏 payload
	if _, err := decodeBlock(data); err == nil {
		t.Fatal("expected corruption error")
	}
	if _, err := decodeBlock(data[:10]); err == nil {
		t.Fatal("expected short-buffer error")
	}
}

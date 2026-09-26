package store

import (
	"math"
	"math/rand"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func benchConfig(dir string) Config {
	return Config{
		Dir:            dir,
		Codec:          codec.Config{Codec: codec.CodecLossless},
		MaxBlockPoints: 4096,
		MinMergeBlocks: 4,
	}
}

func BenchmarkWriteSequential(b *testing.B) {
	s, err := Open(benchConfig(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	b.ResetTimer()
	ts := int64(0)
	for i := 0; i < b.N; i++ {
		ts += 1_000_000_000
		if err := s.Write("m", codec.Point{TS: ts, V: float64(i) * 0.7}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQueryRange(b *testing.B) {
	s, err := Open(benchConfig(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	const total = 200_000
	rng := rand.New(rand.NewSource(1))
	for base := 0; base < total; base += 1000 {
		pts := make([]codec.Point, 1000)
		for i := range pts {
			pts[i] = codec.Point{TS: int64(base + i), V: 20 + math.Sin(float64(base+i)/100)*5 + rng.Float64()}
		}
		if err := s.Write("m", pts...); err != nil {
			b.Fatal(err)
		}
	}
	s.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := int64((i * 997) % (total - 5000))
		if _, err := s.Query("m", start, start+5000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeLossless(b *testing.B) {
	pts := make([]codec.Point, 4096)
	for i := range pts {
		pts[i] = codec.Point{TS: int64(i) * 1_000_000_000, V: 20 + math.Sin(float64(i)/100)*5}
	}
	cfg := codec.Config{Codec: codec.CodecLossless}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeBlock(cfg, pts); err != nil {
			b.Fatal(err)
		}
	}
}

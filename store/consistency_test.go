package store

import (
	"math"
	"math/rand"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// TestQueryMatchesFullReplay: random out-of-order writes with
// duplicates, random flushes and compactions; every random range query
// must match a full replay of the write log (last write wins).
func TestQueryMatchesFullReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 50
	cfg.MinMergeBlocks = 3
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rng := rand.New(rand.NewSource(99))
	model := newReplayModel()
	const horizon = 2000 // timestamps land in [0, horizon)
	for w := 0; w < 3000; w++ {
		var batch []codec.Point
		for i := 0; i < 1+rng.Intn(8); i++ {
			p := codec.Point{TS: int64(rng.Intn(horizon)), V: rng.NormFloat64()}
			batch = append(batch, p)
		}
		if err := s.Write("m", batch...); err != nil {
			t.Fatal(err)
		}
		model.write("m", batch...)
		if rng.Intn(20) == 0 {
			s.Flush()
		}
		if rng.Intn(50) == 0 {
			s.CompactOnce()
		}
	}
	s.Flush()

	// random range queries, including ranges aligned to block boundaries
	for q := 0; q < 300; q++ {
		a := int64(rng.Intn(horizon + 1))
		b := int64(rng.Intn(horizon + 1))
		if a > b {
			a, b = b, a
		}
		got, err := s.Query("m", a, b)
		if err != nil {
			t.Fatal(err)
		}
		want := model.query("m", a, b)
		if !pointsEqual(got, want) {
			t.Fatalf("range [%d,%d): got %d points, want %d", a, b, len(got), len(want))
		}
	}
	// full range
	got, _ := s.Query("m", math.MinInt64, math.MaxInt64)
	if !pointsEqual(got, model.query("m", math.MinInt64, math.MaxInt64)) {
		t.Fatal("full-range mismatch")
	}
}

// TestLossyStoreEndToEnd: lossy mode bounds the error end to end, and
// changing the codec config between restarts keeps old data decodable.
func TestLossyStoreEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 64

	// phase 1: lossless
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(4))
	var exact []codec.Point
	for i := 0; i < 200; i++ {
		p := codec.Point{TS: int64(i), V: rng.NormFloat64() * 100}
		exact = append(exact, p)
	}
	s.Write("m", exact...)
	s.Flush()
	s.Close()

	// phase 2: reopen with lossy config; old data stays exact, new data
	// is bounded by epsilon
	cfg.Codec = codec.Config{Codec: codec.CodecLossy, Epsilon: 0.25}
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s2.Query("m", 0, 200)
	if !pointsEqual(got, exact) {
		t.Fatal("lossless history changed after config switch")
	}
	var lossy []codec.Point
	for i := 200; i < 400; i++ {
		p := codec.Point{TS: int64(i), V: rng.NormFloat64() * 100}
		lossy = append(lossy, p)
	}
	s2.Write("m", lossy...)
	s2.Flush()
	s2.CompactOnce() // merge lossless + lossy blocks; must not degrade
	s2.Close()

	// phase 3: reopen with lossless config; everything still decodable
	cfg.Codec = codec.Config{Codec: codec.CodecLossless}
	s3, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	got, _ = s3.Query("m", 0, 200)
	if !pointsEqual(got, exact) {
		t.Fatal("lossless history degraded")
	}
	got, _ = s3.Query("m", 200, 400)
	if len(got) != len(lossy) {
		t.Fatalf("got %d points, want %d", len(got), len(lossy))
	}
	for i, p := range got {
		if d := math.Abs(p.V - lossy[i].V); d > 0.25 {
			t.Fatalf("point %d error %v exceeds epsilon", i, d)
		}
	}
}

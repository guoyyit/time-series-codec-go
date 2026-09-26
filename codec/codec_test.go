package codec

import (
	"math"
	"math/rand"
	"testing"
)

func makeSeries(n int, startTS, step int64, valFn func(i int) float64) []Point {
	pts := make([]Point, n)
	for i := range pts {
		pts[i] = Point{TS: startTS + int64(i)*step, V: valFn(i)}
	}
	return pts
}

func TestLosslessRoundTripExact(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cases := [][]Point{
		{{TS: 1000, V: 1.5}},
		{{TS: 1000, V: 0}, {TS: 2000, V: 0}},
		makeSeries(100, 1_700_000_000_000_000_000, 1_000_000_000, func(i int) float64 {
			return float64(i) * 1.13
		}),
		makeSeries(1000, 1_700_000_000_000_000_000, 15_000_000_000, func(i int) float64 {
			return math.Sin(float64(i)/10) * 100
		}),
		// irregular timestamps
		{{TS: 10, V: 1}, {TS: 11, V: 2}, {TS: 100, V: 3}, {TS: 1 << 40, V: 4}},
		// special values
		{
			{TS: 1, V: math.NaN()},
			{TS: 2, V: math.Inf(1)},
			{TS: 3, V: math.Inf(-1)},
			{TS: 4, V: 0},
			{TS: 5, V: math.Copysign(0, -1)},
			{TS: 6, V: math.SmallestNonzeroFloat64},
			{TS: 7, V: math.MaxFloat64},
		},
		// random values
		makeSeries(500, 1_000_000, 777, func(i int) float64 {
			return rng.NormFloat64() * 1e6
		}),
		// constant values (best case for XOR)
		makeSeries(200, 5, 10, func(i int) float64 { return 3.14 }),
	}
	cfg := Config{Codec: CodecLossless}
	for ci, pts := range cases {
		blk, err := EncodeBlock(cfg, pts)
		if err != nil {
			t.Fatalf("case %d: encode: %v", ci, err)
		}
		got, err := DecodeBlock(blk)
		if err != nil {
			t.Fatalf("case %d: decode: %v", ci, err)
		}
		if len(got) != len(pts) {
			t.Fatalf("case %d: count %d != %d", ci, len(got), len(pts))
		}
		for i := range pts {
			if got[i].TS != pts[i].TS {
				t.Fatalf("case %d point %d: ts %d != %d", ci, i, got[i].TS, pts[i].TS)
			}
			// bit-exact comparison (handles NaN and -0 correctly)
			if math.Float64bits(got[i].V) != math.Float64bits(pts[i].V) {
				t.Fatalf("case %d point %d: value %v != %v", ci, i, got[i].V, pts[i].V)
			}
		}
	}
}

func TestLossyErrorBound(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const eps = 0.5
	cfg := Config{Codec: CodecLossy, Epsilon: eps}
	pts := makeSeries(2000, 1_700_000_000_000_000_000, 1_000_000_000, func(i int) float64 {
		return rng.NormFloat64() * 1000
	})
	blk, err := EncodeBlock(cfg, pts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlock(blk)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(pts) {
		t.Fatalf("count %d != %d", len(got), len(pts))
	}
	for i := range pts {
		if got[i].TS != pts[i].TS {
			t.Fatalf("point %d: ts mismatch (timestamps must be lossless)", i)
		}
		if d := math.Abs(got[i].V - pts[i].V); d > eps {
			t.Fatalf("point %d: error %v exceeds epsilon %v (orig %v got %v)", i, d, eps, pts[i].V, got[i].V)
		}
	}
}

func TestLossySpecialValuesExact(t *testing.T) {
	cfg := Config{Codec: CodecLossy, Epsilon: 0.1}
	pts := []Point{
		{TS: 1, V: math.NaN()},
		{TS: 2, V: math.Inf(1)},
		{TS: 3, V: math.Inf(-1)},
		{TS: 4, V: 1e30}, // overflows quantization range for small eps
		{TS: 5, V: 0.333},
	}
	blk, err := EncodeBlock(cfg, pts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlock(blk)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if math.Float64bits(got[i].V) != math.Float64bits(pts[i].V) {
			t.Fatalf("point %d: special value not preserved exactly: %v != %v", i, got[i].V, pts[i].V)
		}
	}
	if d := math.Abs(got[4].V - 0.333); d > 0.1 {
		t.Fatalf("error %v exceeds epsilon", d)
	}
}

func TestBlockInfoAndCorruption(t *testing.T) {
	cfg := Config{Codec: CodecLossless}
	pts := makeSeries(10, 12345, 100, func(i int) float64 { return float64(i) })
	blk, err := EncodeBlock(cfg, pts)
	if err != nil {
		t.Fatal(err)
	}
	inf, err := BlockInfo(blk)
	if err != nil {
		t.Fatal(err)
	}
	if inf.Count != 10 || inf.T0 != 12345 || inf.T1 != 12345+900 || inf.Codec != CodecLossless || inf.Epsilon != 0 {
		t.Fatalf("bad info: %+v", inf)
	}
	// flip a payload byte -> CRC must catch it
	blk[headerSize] ^= 0xFF
	if _, err := DecodeBlock(blk); err != ErrCorruptBlock {
		t.Fatalf("expected ErrCorruptBlock, got %v", err)
	}
	// truncated
	if _, err := DecodeBlock(blk[:headerSize+2]); err != ErrCorruptBlock {
		t.Fatalf("expected ErrCorruptBlock for truncated block, got %v", err)
	}
}

func TestEncodeRejectsBadInput(t *testing.T) {
	if _, err := EncodeBlock(Config{Codec: CodecLossless}, nil); err != ErrEmptyInput {
		t.Fatalf("got %v", err)
	}
	dup := []Point{{TS: 1, V: 1}, {TS: 1, V: 2}}
	if _, err := EncodeBlock(Config{Codec: CodecLossless}, dup); err != ErrUnsortedInput {
		t.Fatalf("got %v", err)
	}
	if _, err := EncodeBlock(Config{Codec: CodecLossy}, []Point{{TS: 1, V: 1}}); err != ErrInvalidConfig {
		t.Fatalf("got %v", err)
	}
	if _, err := EncodeBlock(Config{Codec: 99}, []Point{{TS: 1, V: 1}}); err != ErrInvalidConfig {
		t.Fatalf("got %v", err)
	}
}

func TestCompressionRatioSanity(t *testing.T) {
	// regular 1s interval, slowly varying values: should compress well
	pts := makeSeries(4096, 1_700_000_000_000_000_000, 1_000_000_000, func(i int) float64 {
		return 20 + math.Sin(float64(i)/100)*5
	})
	blk, err := EncodeBlock(Config{Codec: CodecLossless}, pts)
	if err != nil {
		t.Fatal(err)
	}
	raw := len(pts) * 16
	if len(blk) >= raw {
		t.Fatalf("no compression: block %d bytes vs raw %d", len(blk), raw)
	}
	t.Logf("lossless: %d bytes for %d points (%.2f bytes/point, raw %d)",
		len(blk), len(pts), float64(len(blk))/float64(len(pts)), raw)

	blkLossy, err := EncodeBlock(Config{Codec: CodecLossy, Epsilon: 0.01}, pts)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lossy: %d bytes (%.2f bytes/point)", len(blkLossy), float64(len(blkLossy))/float64(len(pts)))
}

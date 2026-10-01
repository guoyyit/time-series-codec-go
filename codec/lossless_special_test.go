package codec

import (
	"math"
	"testing"
)

// 无损模式下 NaN/±Inf 经写入、封块、崩溃重启、合并后逐比特保留。
func TestLosslessSpecialValuesFullPath(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	cfg.BlockMaxSamples = 16
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	values := []float64{
		math.NaN(),
		math.Float64frombits(0x7ff8000000000005),
		math.Inf(1),
		math.Inf(-1),
		1e308,
		-1e308,
		42.5,
		-0.0,
	}
	for i, v := range values {
		if err := s.Write(int64(i), v); err != nil {
			t.Fatal(err)
		}
	}
	// 再写多点触发封块，特殊值位于块边界附近。
	for i := len(values); i < 40; i++ {
		s.Write(int64(i), float64(i))
	}
	s.Flush()

	// 崩溃重启。
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		s2.MergeNow()
	}
	got, err := s2.Query(0, int64(len(values)-1))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range values {
		if math.Float64bits(got[i].V) != math.Float64bits(want) {
			t.Fatalf("sample %d: got %#x want %#x", i, math.Float64bits(got[i].V), math.Float64bits(want))
		}
	}
	s2.Close()
}

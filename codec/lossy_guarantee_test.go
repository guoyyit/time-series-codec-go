package codec

import (
	"hash/crc32"
	"math"
	"testing"
)

// sameOrBounded 校验：特殊值必须逐比特一致；有限值误差不得超过 eps。
func sameOrBounded(t *testing.T, i int, got, orig float64, eps float64) {
	t.Helper()
	switch {
	case math.IsNaN(orig):
		if !math.IsNaN(got) {
			t.Fatalf("sample %d: NaN not preserved, got %v", i, got)
		}
	case math.IsInf(orig, 1):
		if !math.IsInf(got, 1) {
			t.Fatalf("sample %d: +Inf not preserved, got %v", i, got)
		}
	case math.IsInf(orig, -1):
		if !math.IsInf(got, -1) {
			t.Fatalf("sample %d: -Inf not preserved, got %v", i, got)
		}
	default:
		if d := math.Abs(got - orig); d > eps {
			t.Fatalf("sample %d (orig=%v): error %v exceeds epsilon %v", i, orig, d, eps)
		}
	}
}

// 有损块必须统一保留 NaN、正负无穷，并对极端量级/极小上界满足承诺。
func TestLossyBlockSpecialAndExtremeValues(t *testing.T) {
	specials := []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		0,
		-0.0,
		1,
		-1,
		1e18,
		-1e18,
		1e308,
		-1e308,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		1e-300,
		123.456789,
		1<<53 + 1, // 2^53+1：超过 epsilon=0.1 的可分辨粒度
	}
	epsilons := []float64{0.1, 0.25, math.SmallestNonzeroFloat64, 1e-300, 1e300}
	for _, eps := range epsilons {
		samples := make([]Sample, len(specials))
		for i, v := range specials {
			samples[i] = Sample{T: int64(i), V: v}
		}
		data, err := encodeBlock(samples, ModeLossy, eps)
		if err != nil {
			t.Fatalf("eps=%v encode: %v", eps, err)
		}
		if data[2] != blockVersion2 {
			t.Fatalf("eps=%v: expected block version 2, got %d", eps, data[2])
		}
		got, err := decodeBlock(data)
		if err != nil {
			t.Fatalf("eps=%v decode: %v", eps, err)
		}
		for i := range samples {
			sameOrBounded(t, i, got[i].V, samples[i].V, eps)
		}
	}
}

// 1e18 + epsilon=0.1 的典型组合：必须逐比特保留，绝不静默超差。
func TestLossyBlockHugeMagnitudeSmallEpsilon(t *testing.T) {
	const eps = 0.1
	values := []float64{1e18, -1e18, 1e18 + 1, -1e18 - 1, 1e18 + 0.05}
	samples := make([]Sample, len(values))
	for i, v := range values {
		samples[i] = Sample{T: int64(i), V: v}
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
		if math.Float64bits(got[i].V) != math.Float64bits(samples[i].V) {
			t.Fatalf("sample %d must be preserved bit-exact: got %v want %v", i, got[i].V, samples[i].V)
		}
	}
}

// 最小正 epsilon：任何普通有限样本都不得超差（不可量化即精确保留）。
func TestLossyBlockMinPositiveEpsilon(t *testing.T) {
	eps := math.SmallestNonzeroFloat64
	values := []float64{0, 1, -1, 1.5, 1e-320, 1e308, 42.0, 43.0}
	samples := make([]Sample, len(values))
	for i, v := range values {
		samples[i] = Sample{T: int64(i), V: v}
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
		sameOrBounded(t, i, got[i].V, samples[i].V, eps)
	}
}

// NaN 的不同载荷也必须原样保留。
func TestLossyBlockNaNPayloadPreserved(t *testing.T) {
	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0x7ff8000000000002)
	samples := []Sample{{T: 1, V: nan1}, {T: 2, V: nan2}}
	data, err := encodeBlock(samples, ModeLossy, 0.1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(got[0].V) != math.Float64bits(nan1) ||
		math.Float64bits(got[1].V) != math.Float64bits(nan2) {
		t.Fatalf("NaN payloads not preserved: %#x %#x",
			math.Float64bits(got[0].V), math.Float64bits(got[1].V))
	}
}

// 相邻可量化样本的 q 分别接近 int64 两端时，增量回绕仍能正确恢复。
func TestLossyBlockQuantDeltaWrapAround(t *testing.T) {
	eps := math.Ldexp(1, 53) // 2^53：使 q 可取到接近 int64 极值
	vHi := float64(math.MaxInt64-1024) * eps
	vLo := float64(math.MinInt64+1024) * eps
	samples := []Sample{{T: 1, V: vHi}, {T: 2, V: vLo}, {T: 3, V: vHi}}
	data, err := encodeBlock(samples, ModeLossy, eps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range samples {
		if math.Float64bits(got[i].V) != math.Float64bits(samples[i].V) {
			t.Fatalf("sample %d mismatch: got %v want %v", i, got[i].V, samples[i].V)
		}
	}
}

// 旧 v1 有损块仍按原规则解码（既有落盘格式不变）。
func TestDecodeV1LossyBlockUnchanged(t *testing.T) {
	// 手工构造 v1 有损块：version=1，值为 q 的 zigzag varint 增量。
	buf := make([]byte, blockHeadSize)
	buf[0] = blockMagic0
	buf[1] = blockMagic1
	buf[2] = blockVersion
	buf[3] = byte(ModeLossy)
	const eps = 0.5
	putF := func(b []byte, v float64) {
		for i := 0; i < 8; i++ {
			b[i] = byte(math.Float64bits(v) >> (8 * i))
		}
	}
	putF(buf[4:12], eps)
	putU32 := func(b []byte, v uint32) {
		for i := 0; i < 4; i++ {
			b[i] = byte(v >> (8 * i))
		}
	}
	putU32(buf[12:16], 3)
	putI64 := func(b []byte, v int64) {
		for i := 0; i < 8; i++ {
			b[i] = byte(uint64(v) >> (8 * i))
		}
	}
	putI64(buf[16:24], 0)
	putI64(buf[24:32], 2)
	// 时间戳流：t=0,1,2 的 dod 为 1, 0。
	buf = appendVarint(buf, zigzag(1))
	buf = appendVarint(buf, zigzag(0))
	// q = round(v/0.5): 0, 1, 2；写入的是 q 增量：0, 1, 1。
	for _, dq := range []int64{0, 1, 1} {
		buf = appendVarint(buf, zigzag(dq))
	}
	crc := crc32.ChecksumIEEE(buf)
	buf = append(buf, byte(crc), byte(crc>>8), byte(crc>>16), byte(crc>>24))

	got, err := decodeBlock(buf)
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{0, 0.5, 1.0}
	for i := range want {
		if got[i].V != want[i] {
			t.Fatalf("v1 lossy decode changed: i=%d got %v want %v", i, got[i].V, want[i])
		}
	}
}

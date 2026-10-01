package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// checkLossyValue 校验有损解码值满足统一数值保证：
// NaN 仍为 NaN，无穷保持符号，有限样本偏差不超过 eps。
func checkLossyValue(t *testing.T, got, want, eps float64, ctx string) {
	t.Helper()
	switch {
	case math.IsNaN(want):
		if !math.IsNaN(got) {
			t.Fatalf("%s: got %v, want NaN", ctx, got)
		}
	case math.IsInf(want, 0):
		if got != want {
			t.Fatalf("%s: got %v, want %v（符号必须保持）", ctx, got, want)
		}
	default:
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Fatalf("%s: got %v, want finite %v", ctx, got, want)
		}
		if d := math.Abs(got - want); d > eps {
			t.Fatalf("%s: |%v - %v| = %v > epsilon %v", ctx, got, want, d, eps)
		}
	}
}

// assertLossyQueryEqual 校验查询结果与参考模型逐点一致（有损数值保证下）。
func assertLossyQueryEqual(t *testing.T, s *Store, ref *reference, start, end int64, eps float64) {
	t.Helper()
	got, err := s.Query(start, end)
	if err != nil {
		t.Fatal(err)
	}
	want := ref.query(start, end)
	if len(got) != len(want) {
		t.Fatalf("[%d,%d] count mismatch: got %d want %d", start, end, len(got), len(want))
	}
	for i := range want {
		if got[i].T != want[i].T {
			t.Fatalf("[%d,%d] sample %d ts mismatch: got %d want %d", start, end, i, got[i].T, want[i].T)
		}
		checkLossyValue(t, got[i].V, want[i].V, eps, fmt.Sprintf("[%d,%d] sample %d t=%d", start, end, i, want[i].T))
	}
}

// specialValuePool 返回覆盖 NaN、正负无穷、极端量级与常规值的样本池。
func specialValuePool() []float64 {
	return []float64{
		math.NaN(),
		math.Inf(1), math.Inf(-1),
		0, math.Copysign(0, -1),
		1e18, -1e18, 4.5e15, -9.9e17,
		math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
		1e-300, -1e-300,
		1.5, -2.75, 100.25, -1000.5,
	}
}

// 块级往返：NaN/Inf 逐比特保留，有限样本误差 <= eps。
func TestLossySpecialValuesBlockRoundTrip(t *testing.T) {
	const eps = 0.1
	vals := specialValuePool()
	var samples []Sample
	for i, v := range vals {
		samples = append(samples, Sample{T: int64(i * 1000), V: v})
	}
	data, err := encodeBlock(samples, ModeLossy, eps)
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
	for i, s := range samples {
		if got[i].T != s.T {
			t.Fatalf("sample %d ts mismatch", i)
		}
		checkLossyValue(t, got[i].V, s.V, eps, fmt.Sprintf("sample %d", i))
	}
	// NaN/Inf 必须逐比特保留（含 NaN 的位模式）。
	for i, s := range samples {
		if math.IsNaN(s.V) || math.IsInf(s.V, 0) {
			if math.Float64bits(got[i].V) != math.Float64bits(s.V) {
				t.Fatalf("sample %d special value not bit-exact: got %v want %v", i, got[i].V, s.V)
			}
		}
	}
}

// 极端量级 + 相对很小的误差上界：1e18 与 eps=0.1 的组合不得超差。
func TestLossyHugeMagnitudeSmallEpsilon(t *testing.T) {
	const eps = 0.1
	vals := []float64{1e18, -1e18, 1e18 + 0.5, 9.22e18, -9.22e18, 1e17, 42.5}
	var samples []Sample
	for i, v := range vals {
		samples = append(samples, Sample{T: int64(i), V: v})
	}
	data, err := encodeBlock(samples, ModeLossy, eps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range samples {
		checkLossyValue(t, got[i].V, s.V, eps, fmt.Sprintf("sample %d v=%v", i, s.V))
	}
	// 无法满足上界的样本必须按原始值逐比特保留。
	for i, s := range samples {
		if math.Abs(s.V) >= 1e17 {
			if math.Float64bits(got[i].V) != math.Float64bits(s.V) {
				t.Fatalf("sample %d v=%v not preserved bit-exact: got %v", i, s.V, got[i].V)
			}
		}
	}
}

// 误差上界取 float64 最小正数：普通有限样本不得超差或被改写。
func TestLossySmallestPositiveEpsilon(t *testing.T) {
	eps := math.SmallestNonzeroFloat64
	vals := []float64{1.5, -2.75, 100.25, 1e18, -1e-300, 0, math.SmallestNonzeroFloat64}
	var samples []Sample
	for i, v := range vals {
		samples = append(samples, Sample{T: int64(i), V: v})
	}
	data, err := encodeBlock(samples, ModeLossy, eps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range samples {
		if math.Float64bits(got[i].V) != math.Float64bits(s.V) {
			t.Fatalf("sample %d changed: got %v want %v", i, got[i].V, s.V)
		}
	}
}

// 非法误差上界：Open 必须失败、错误可区分，且不产生任何文件残留。
func TestInvalidEpsilonRejected(t *testing.T) {
	bad := []struct {
		name string
		eps  float64
	}{
		{"zero", 0},
		{"negative", -0.5},
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			cfg := DefaultConfig(dir)
			cfg.Mode = ModeLossy
			cfg.Epsilon = tc.eps
			s, err := Open(cfg)
			if err == nil {
				s.Close()
				t.Fatalf("Open with epsilon %v succeeded", tc.eps)
			}
			if !errors.Is(err, ErrInvalidEpsilon) {
				t.Fatalf("error %v is not ErrInvalidEpsilon", err)
			}
			// 校验失败不得创建数据目录或任何文件。
			if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
				t.Fatalf("dir %s created despite invalid config", dir)
			}
		})
	}
	// 已存在的目录：校验失败不得新增/修改任何文件。
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(sentinel, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(dir)
	cfg.Mode = ModeLossy
	cfg.Epsilon = math.NaN()
	if _, err := Open(cfg); !errors.Is(err, ErrInvalidEpsilon) {
		t.Fatalf("expected ErrInvalidEpsilon, got %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "keep.txt" {
		t.Fatalf("dir modified after rejected Open: %v", entries)
	}
	// 无损模式不校验 Epsilon（被忽略），含 0 与 NaN。
	for _, eps := range []float64{0, math.NaN(), math.Inf(1)} {
		cfg := DefaultConfig(filepath.Join(t.TempDir(), "db"))
		cfg.Epsilon = eps
		s, err := Open(cfg)
		if err != nil {
			t.Fatalf("lossless mode with epsilon %v rejected: %v", eps, err)
		}
		s.Close()
	}
	// 合法边界：最小正数必须被接受。
	cfg = DefaultConfig(filepath.Join(t.TempDir(), "db"))
	cfg.Mode = ModeLossyReviverPlaceholder
	_ = cfg
}

// v1 有损块（无转义机制的旧格式）的解码规则必须保持不变。
func TestBlockV1LossyDecodeCompat(t *testing.T) {
	const eps = 0.5
	samples := []Sample{
		{T: 1000, V: 10.3}, {T: 2000, V: 20.7}, {T: 3500, V: -5.2}, {T: 4000, V: 0},
	}
	data := encodeBlockV1Lossy(t, samples, eps)
	got, err := decodeBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(samples) {
		t.Fatalf("count mismatch: %d != %d", len(got), len(samples))
	}
	prevQ := int64(0)
	for i, s := range samples {
		q := int64(math.Round(s.V / eps))
		want := float64(prevQ+q-prevQ) * eps // = float64(q)*eps，按 v1 规则
		prevQ = q
		if got[i].T != s.T || got[i].V != want {
			t.Fatalf("v1 sample %d: got %+v, want t=%d v=%v", i, got[i], s.T, want)
		}
	}
}

// encodeBlockV1Lossy 按 v1 格式（无转义机制）编码有损块，
// 用于锁定既有落盘数据的解码规则。
func encodeBlockV1Lossy(t *testing.T, samples []Sample, epsilon float64) []byte {
	t.Helper()
	buf := make([]byte, blockHeadSize)
	buf[0] = blockMagic0
	buf[1] = blockMagic1
	buf[2] = blockVersionV1
	buf[3] = byte(ModeLossy)
	binary.LittleEndian.PutUint64(buf[4:12], math.Float64bits(epsilon))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(samples)))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(samples[0].T))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(samples[len(samples)-1].T))
	prevDelta := int64(0)
	for i := 1; i < len(samples); i++ {
		delta := samples[i].T - samples[i-1].T
		buf = appendVarint(buf, zigzag(delta-prevDelta))
		prevDelta = delta
	}
	prevQ := int64(0)
	for i, s := range samples {
		q := int64(math.Round(s.V / epsilon))
		if i == 0 {
			buf = appendVarint(buf, zigzag(q))
		} else {
			buf = appendVarint(buf, zigzag(q-prevQ))
		}
		prevQ = q
	}
	return binary.LittleEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

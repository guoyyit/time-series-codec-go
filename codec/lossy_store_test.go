package codec

import (
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// assertRefLossy 逐点比较 store 与参考模型：特殊值逐比特一致，
// 有限值误差不超过 eps。
func assertRefLossy(t *testing.T, s *Store, ref map[int64]float64, start, end int64, eps float64) {
	t.Helper()
	got, err := s.Query(start, end)
	if err != nil {
		t.Fatal(err)
	}
	var want []Sample
	for ts, v := range ref {
		if ts >= start && ts <= end {
			want = append(want, Sample{T: ts, V: v})
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i].T < want[j].T })
	if len(got) != len(want) {
		t.Fatalf("[%d,%d] count mismatch: got %d want %d", start, end, len(got), len(want))
	}
	for i := range want {
		if got[i].T != want[i].T {
			t.Fatalf("ts mismatch at %d: got %d want %d", i, got[i].T, want[i].T)
		}
		g, w := got[i].V, want[i].V
		if math.IsNaN(w) || math.IsInf(w, 0) {
			if math.Float64bits(g) != math.Float64bits(w) {
				t.Fatalf("t=%d special value not preserved: got %v(%#x) want %v(%#x)",
					want[i].T, g, math.Float64bits(g), w, math.Float64bits(w))
			}
			continue
		}
		if d := math.Abs(g - w); d > eps {
			t.Fatalf("t=%d error %v exceeds epsilon %v (got %v want %v)", want[i].T, d, eps, g, w)
		}
	}
}

func specialValuesPool() []float64 {
	return []float64{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
		math.Float64frombits(0x7ff8000000000001),
		0,
		-0.0,
		1,
		-1,
		0.5,
		3.14159,
		1e18,
		-1e18,
		1e18 + 7,
		1e308,
		-1e308,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		1e-300,
	}
}

// 特殊值与极端量级样本：写入、封块、重启恢复、后台合并、乱序去重、
// 块边界区间查询，全程与参考模型一致。
func TestLossySpecialValuesFullLifecycle(t *testing.T) {
	const eps = 0.1
	cfg := lossyCfg(t, eps)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ref := map[int64]float64{}
	pool := specialValuesPool()

	// 1) 顺序写满并跨多个块边界。
	for i := 0; i < 300; i++ {
		v := pool[i%len(pool)]
		s.Write(int64(i), v)
		ref[int64(i)] = v
	}
	// 2) 乱序/迟到写入与重复覆盖（含特殊值覆盖普通值、反之亦然）。
	for _, i := range []int64{0, 1, 36, 37, 73, 100, 299} {
		v := pool[int(i*3)%len(pool)]
		s.Write(i, v)
		ref[i] = v
	}
	// 特殊值覆盖已有时间戳，再用普通值覆盖回来。
	s.Write(5, math.Inf(1))
	ref[5] = math.Inf(1)
	s.Write(5, math.NaN())
	ref[5] = math.NaN()
	s.Write(5, 42.0)
	ref[5] = 42.0

	assertRefLossy(t, s, ref, 0, 299, eps)

	// 3) 重启恢复（模拟崩溃：不 Close，直接重开）。
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertRefLossy(t, s2, ref, 0, 299, eps)

	// 4) 继续写入并封块后多轮合并。
	for i := 300; i < 400; i++ {
		v := pool[(i+5)%len(pool)]
		s2.Write(int64(i), v)
		ref[int64(i)] = v
	}
	if err := s2.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := s2.MergeNow(); err != nil {
			t.Fatal(err)
		}
	}
	assertRefLossy(t, s2, ref, 0, 399, eps)

	// 5) 块边界与跨边界区间查询。
	for b := int64(0); b <= 400; b += int64(cfg.BlockMaxSamples) {
		assertRefLossy(t, s2, ref, b-1, b+1, eps)
		assertRefLossy(t, s2, ref, b, b, eps)
	}
	assertRefLossy(t, s2, ref, -100, 1<<60, eps)

	// 6) 再次重启。
	s2.Close()
	s3, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertRefLossy(t, s3, ref, 0, 399, eps)
}

// 配置切换：旧有损块（v2，含特殊值）在新 epsilon 下解码保真不变；
// 旧无损块规则也不受影响。
func TestLossyConfigChangeSpecialValues(t *testing.T) {
	cfg := lossyCfg(t, 0.1)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	specials := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1e18, -1e18}
	exact := map[int64]float64{}
	for i, v := range specials {
		s.Write(int64(i), v)
		exact[int64(i)] = v
	}
	s.Flush()
	s.Close()

	// 用不同 epsilon 重开：特殊值与极端值必须仍逐比特一致。
	cfg.Epsilon = 1.25
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Query(0, int64(len(specials)-1))
	if err != nil {
		t.Fatal(err)
	}
	for i, smp := range got {
		if math.Float64bits(smp.V) != math.Float64bits(specials[i]) {
			t.Fatalf("after config change t=%d: got %#x want %#x",
				smp.T, math.Float64bits(smp.V), math.Float64bits(specials[i]))
		}
	}
}

// 随机生成含特殊值与极端量级的乱序写入，随机封块/合并/重启，
// 用随机区间与参考模型做交叉验证。
func TestLossyRandomSpecialCrossValidation(t *testing.T) {
	const eps = 0.25
	cfg := lossyCfg(t, eps)
	cfg.BlockMaxSamples = 23
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(20251002))
	ref := map[int64]float64{}
	genValue := func() float64 {
		switch rng.Intn(10) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1)
		case 2:
			return math.Inf(-1)
		case 3:
			return 1e18 + float64(rng.Intn(10000))
		case 4:
			return -(1e18 + float64(rng.Intn(10000)))
		case 5:
			return math.MaxFloat64 / (1 + float64(rng.Intn(100)))
		case 6:
			return math.SmallestNonzeroFloat64 * float64(1+rng.Intn(100))
		default:
			return rng.NormFloat64() * 100
		}
	}

	const tsRange = 300
	checkRandomRanges := func(s *Store, rounds int) {
		t.Helper()
		for i := 0; i < rounds; i++ {
			a := int64(rng.Intn(tsRange + 50))
			b := a + int64(rng.Intn(60))
			assertRefLossy(t, s, ref, a, b, eps)
		}
	}

	for round := 0; round < 6; round++ {
		n := 80 + rng.Intn(80)
		for i := 0; i < n; i++ {
			ts := int64(rng.Intn(tsRange))
			v := genValue()
			if err := s.Write(ts, v); err != nil {
				t.Fatal(err)
			}
			ref[ts] = v
		}
		// 随机触发封块、合并。
		if rng.Intn(2) == 0 {
			if err := s.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 2; i++ {
			if err := s.MergeNow(); err != nil {
				t.Fatal(err)
			}
		}
		checkRandomRanges(s, 20)

		// 模拟崩溃重启：不 Close，直接重开。
		if round%2 == 1 {
			s, err = Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	s.Flush()
	for i := 0; i < 5; i++ {
		s.MergeNow()
	}
	checkRandomRanges(s, 100)
	assertRefLossy(t, s, ref, 0, tsRange, eps)
	s.Close()

	// 最终重启后再验证一次。
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertRefLossy(t, s, ref, 0, tsRange, eps)
}

// 连续多轮崩溃与重启（含未 Close 的 WAL 重放与已封块混合），
// 数据始终与参考模型一致。
func TestLossyRepeatedCrashRestart(t *testing.T) {
	const eps = 0.1
	cfg := lossyCfg(t, eps)
	cfg.BlockMaxSamples = 31
	rng := rand.New(rand.NewSource(777))
	ref := map[int64]float64{}

	genValue := func() float64 {
		switch rng.Intn(8) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1 - 2*rng.Intn(2))
		case 2:
			return 1e18 + float64(rng.Intn(1000))
		default:
			return rng.NormFloat64() * 10
		}
	}

	var s *Store
	for round := 0; round < 8; round++ {
		var err error
		s, err = Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			ts := int64(rng.Intn(500))
			v := genValue()
			if err := s.Write(ts, v); err != nil {
				t.Fatal(err)
			}
			ref[ts] = v
		}
		if round%3 == 0 {
			s.Flush()
		}
		// 直接丢弃 s（不 Close），模拟崩溃。
		s = nil
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertRefLossy(t, s, ref, 0, 499, eps)
}

// 合并期间并发写入与查询，特殊值场景下不报错、结果始终有序去重，
// 最终与参考模型逐点一致；合并前后相同区间结果一致。
func TestLossyMergeConcurrentSpecial(t *testing.T) {
	const eps = 0.1
	cfg := lossyCfg(t, eps)
	cfg.BlockMaxSamples = 29
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ref := map[int64]float64{}
	var refMu sync.Mutex
	var writerWg, mergerWg sync.WaitGroup
	stop := make(chan struct{})

	genValue := func(r *rand.Rand) float64 {
		switch r.Intn(7) {
		case 0:
			return math.NaN()
		case 1:
			return math.Inf(1)
		case 2:
			return math.Inf(-1)
		case 3:
			return 1e18 + float64(r.Intn(100))
		default:
			return r.NormFloat64()
		}
	}

	for w := 0; w < 3; w++ {
		writerWg.Add(1)
		go func(seed int64) {
			defer writerWg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 400; i++ {
				ts := int64(r.Intn(800))
				v := genValue(r)
				if err := s.Write(ts, v); err != nil {
					return
				}
				refMu.Lock()
				ref[ts] = v
				refMu.Unlock()
			}
		}(int64(100 + w))
	}
	mergerWg.Add(1)
	go func() {
		defer mergerWg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
				s.MergeNow()
			}
		}
	}()
	for q := 0; q < 2; q++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for i := 0; i < 80; i++ {
				got, err := s.Query(0, 800)
				if err != nil {
					return
				}
				for j := 1; j < len(got); j++ {
					if got[j].T <= got[j-1].T {
						t.Errorf("query not strictly increasing")
						return
					}
				}
			}
		}()
	}
	writerWg.Wait()
	close(stop)
	mergerWg.Wait()

	s.Flush()
	refMu.Lock()
	defer refMu.Unlock()
	assertRefLossy(t, s, ref, 0, 800, eps)

	// 合并前后相同区间结果一致。
	before, err := s.Query(0, 800)
	if err != nil {
		t.Fatal(err)
	}
	s.MergeNow()
	s.MergeNow()
	after, err := s.Query(0, 800)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("merge changed result count: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if math.Float64bits(before[i].V) != math.Float64bits(after[i].V) || before[i].T != after[i].T {
			t.Fatalf("merge changed result at %d: %+v -> %+v", i, before[i], after[i])
		}
	}
}

func lossyCfg(t *testing.T, eps float64) Config {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.Mode = ModeLossy
	cfg.Epsilon = eps
	cfg.BlockMaxSamples = 37
	cfg.SyncWrites = false
	return cfg
}

// 非法 epsilon 必须被拒绝（错误可用 errors.Is 区分），且不得创建
// 或修改数据目录下的任何文件或目录。
func TestInvalidEpsilonRejected(t *testing.T) {
	bad := []float64{0, math.Copysign(0, -1), -0.1, math.NaN(), math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64 * -1}
	for _, eps := range bad {
		dir := filepath.Join(t.TempDir(), "data")
		cfg := DefaultConfig(dir)
		cfg.Mode = ModeLossy
		cfg.Epsilon = eps
		s, err := Open(cfg)
		if err == nil {
			s.Close()
			t.Fatalf("epsilon=%v: expected error", eps)
		}
		if !errors.Is(err, ErrInvalidEpsilon) {
			t.Fatalf("epsilon=%v: err %v does not wrap ErrInvalidEpsilon", eps, err)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("epsilon=%v: err %v does not wrap ErrInvalidConfig", eps, err)
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Fatalf("epsilon=%v: data dir was created/modified: %v", eps, statErr)
		}
	}
}

// 合法边界值（最小正 epsilon）必须能正常打开。
func TestMinPositiveEpsilonAccepted(t *testing.T) {
	cfg := lossyCfg(t, math.SmallestNonzeroFloat64)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

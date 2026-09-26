package codec

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T, mode Mode) Config {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.Mode = mode
	cfg.Epsilon = 0.25
	cfg.BlockMaxSamples = 64
	cfg.SyncWrites = false // 测试吞吐优先；恢复语义不依赖 fsync
	return cfg
}

// reference 是"全量重放 + 后写覆盖"的参考模型。
type reference struct{ m map[int64]float64 }

func (r *reference) write(t int64, v float64) { r.m[t] = v }

func (r *reference) query(start, end int64) []Sample {
	var out []Sample
	for t, v := range r.m {
		if t >= start && t <= end {
			out = append(out, Sample{T: t, V: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func assertQueryEqual(t *testing.T, s *Store, ref *reference, start, end int64, lossy bool, eps float64) {
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
		if lossy {
			if d := math.Abs(got[i].V - want[i].V); d > eps {
				t.Fatalf("sample %d error %v > epsilon %v", i, d, eps)
			}
		} else if math.Float64bits(got[i].V) != math.Float64bits(want[i].V) {
			t.Fatalf("sample %d value mismatch: got %v want %v", i, got[i].V, want[i].V)
		}
	}
}

// 无损一致性 + 乱序/重复/迟到写入 + 任意区间查询。
func TestLosslessOutOfOrderDedup(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ref := &reference{m: map[int64]float64{}}
	rng := rand.New(rand.NewSource(1))

	// 顺序写入 500 个点。
	for i := 0; i < 500; i++ {
		v := rng.NormFloat64() * 100
		s.Write(int64(i*1000), v)
		ref.write(int64(i*1000), v)
	}
	// 乱序迟到 + 重复（含不同值的重复，后写应覆盖）。
	for i := 0; i < 200; i++ {
		ts := int64(rng.Intn(500)) * 1000
		v := rng.NormFloat64() * 100
		s.Write(ts, v)
		ref.write(ts, v)
	}
	// 完全重复写入（同值）。
	for i := 0; i < 50; i++ {
		ts := int64(rng.Intn(500)) * 1000
		v := ref.m[ts]
		s.Write(ts, v)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// 任意区间查询与全量重放一致（覆盖块边界）。
	for _, r := range [][2]int64{{0, 499000}, {100000, 200000}, {64000, 64000}, {999, 1001}, {-1000, 0}, {499000, 1 << 60}} {
		assertQueryEqual(t, s, ref, r[0], r[1], false, 0)
	}
}

// 有损模式：误差不超过上界，乱序去重仍正确。
func TestLossyBoundedError(t *testing.T) {
	cfg := testConfig(t, ModeLossy)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ref := &reference{m: map[int64]float64{}}
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 1000; i++ {
		ts := int64(rng.Intn(2000)) * 10 // 大量乱序与重复
		v := rng.Float64() * 1000
		s.Write(ts, v)
		ref.write(ts, v)
	}
	s.Flush()
	assertQueryEqual(t, s, ref, 0, 20000, true, cfg.Epsilon)
}

// 崩溃恢复：不 Close 直接重开，WAL 重放后数据完整。
func TestCrashRecovery(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ref := &reference{m: map[int64]float64{}}
	rng := rand.New(rand.NewSource(3))
	// 部分样本已封块，部分仍在 WAL。
	for i := 0; i < 300; i++ {
		v := rng.Float64()
		s.Write(int64(i), v)
		ref.write(int64(i), v)
	}
	// 模拟崩溃：不调用 Close，直接重新 Open。
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertQueryEqual(t, s2, ref, 0, 299, false, 0)

	// 重启后继续写入，数据仍一致。
	for i := 300; i < 400; i++ {
		v := rng.Float64()
		s2.Write(int64(i), v)
		ref.write(int64(i), v)
	}
	s2.Flush()
	assertQueryEqual(t, s2, ref, 0, 399, false, 0)
}

// 崩溃残留：孤儿块文件与损坏的 WAL 尾部不影响恢复。
func TestCrashRecoveryOrphansAndTornWal(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		s.Write(int64(i), float64(i))
	}
	s.Flush()
	for i := 100; i < 150; i++ {
		s.Write(int64(i), float64(i))
	}
	// 模拟崩溃：留下孤儿块文件 + WAL 尾部半条记录。
	orphan := filepath.Join(cfg.Dir, blocksDirName, "00000000000000ff.blk")
	os.WriteFile(orphan, []byte("garbage"), 0o644)
	walPath := filepath.Join(cfg.Dir, walFileName)
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0xde, 0xad, 0xbe}) // 半条记录
	f.Close()

	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Query(0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 150 {
		t.Fatalf("expected 150 samples after recovery, got %d", len(got))
	}
	for i, smp := range got {
		if smp.T != int64(i) || smp.V != float64(i) {
			t.Fatalf("sample %d mismatch: %+v", i, smp)
		}
	}
	// 孤儿文件应已被清理。
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("orphan block file not cleaned")
	}
}

// 后台合并：块数减少，数据不丢，查询结果不变。
func TestMergeReducesBlocks(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	cfg.Merge.MinBlocks = 2
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ref := &reference{m: map[int64]float64{}}
	rng := rand.New(rand.NewSource(4))
	// 乱序写入制造许多重叠小块。
	for i := 0; i < 40; i++ {
		for j := 0; j < 10; j++ {
			ts := int64(rng.Intn(500))
			v := rng.Float64()
			s.Write(ts, v)
			ref.write(ts, v)
		}
		s.Flush() // 每批封一个小块
	}
	s.mu.RLock()
	before := len(s.ix.blocks)
	s.mu.RUnlock()
	if before < 2 {
		t.Fatalf("expected multiple blocks, got %d", before)
	}
	for i := 0; i < 10; i++ {
		if err := s.MergeNow(); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.RLock()
	after := len(s.ix.blocks)
	s.mu.RUnlock()
	if after >= before {
		t.Fatalf("merge did not reduce blocks: before=%d after=%d", before, after)
	}
	assertQueryEqual(t, s, ref, 0, 500, false, 0)
	t.Logf("blocks: %d -> %d", before, after)
}

// 合并期间并发写入与查询：结果始终与参考模型一致。
func TestMergeConcurrentReadWrite(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	cfg.BlockMaxSamples = 32
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ref := &reference{m: map[int64]float64{}}
	var refMu sync.Mutex
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 写入协程：持续乱序写入。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 500; i++ {
				ts := int64(rng.Intn(5000))
				v := rng.Float64()
				if err := s.Write(ts, v); err != nil {
					return
				}
				refMu.Lock()
				ref.write(ts, v)
				refMu.Unlock()
			}
		}(10 + int64(w))
	}
	// 合并协程。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				s.MergeNow()
			}
		}
	}()
	// 查询协程：只验证不报错、结果有序且时间戳去重。
	var qwg sync.WaitGroup
	for q := 0; q < 2; q++ {
		qwg.Add(1)
		go func() {
			defer qwg.Done()
			for i := 0; i < 50; i++ {
				got, err := s.Query(0, 5000)
				if err != nil {
					return
				}
				for j := 1; j < len(got); j++ {
					if got[j].T <= got[j-1].T {
						t.Errorf("query result not strictly increasing")
						return
					}
				}
			}
		}()
	}
	qwg.Wait()
	close(stop)
	wg.Wait()

	// 最终一致性：全量查询 == 参考模型。
	s.Flush()
	refMu.Lock()
	assertQueryEqual(t, s, ref, 0, 5000, false, 0)
	refMu.Unlock()
}

// 配置变更：旧块（无损）在新配置（有损）下仍可精确解码。
func TestConfigChangeKeepsOldDataDecodable(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var exact []Sample
	for i := 0; i < 100; i++ {
		v := float64(i) * 1.23456789
		s.Write(int64(i), v)
		exact = append(exact, Sample{T: int64(i), V: v})
	}
	s.Flush()
	s.Close()

	// 以有损配置重开：旧数据必须无损解码。
	cfg.Mode = ModeLossy
	cfg.Epsilon = 0.5
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Query(0, 99)
	if err != nil {
		t.Fatal(err)
	}
	for i := range exact {
		if math.Float64bits(got[i].V) != math.Float64bits(exact[i].V) {
			t.Fatalf("old lossless data corrupted at %d: got %v want %v", i, got[i].V, exact[i].V)
		}
	}
	// 新写入按有损编码。
	for i := 100; i < 200; i++ {
		s2.Write(int64(i), float64(i)*3.7)
	}
	s2.Flush()
	got, _ = s2.Query(100, 199)
	for i, smp := range got {
		if d := math.Abs(smp.V - float64(i+100)*3.7); d > 0.5 {
			t.Fatalf("new lossy sample exceeds bound: %v", d)
		}
	}
}

// 压缩率粗测：常规时序数据应明显小于原始大小。
func TestCompressionRatio(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v := 20.0
	rng := rand.New(rand.NewSource(5))
	n := 4000
	for i := 0; i < n; i++ {
		v += rng.NormFloat64() * 0.1
		s.Write(int64(1700000000000+i*1000), v)
	}
	s.Flush()
	var total int64
	filepath.Walk(cfg.Dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	raw := int64(n * 16)
	t.Logf("raw=%dB stored=%dB ratio=%.1fx", raw, total, float64(raw)/float64(total))
	if total >= raw {
		t.Fatalf("no compression: stored %d >= raw %d", total, raw)
	}
}

func TestQueryEmptyAndClosed(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	s, _ := Open(cfg)
	got, err := s.Query(0, 100)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty query: got %v err %v", got, err)
	}
	s.Write(1, 1)
	s.Close()
	if _, err := s.Query(0, 1); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := s.Write(2, 2); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

// 区间边界：块边界与乱序样本附近的查询与全量重放一致。
func TestBlockBoundaryQueries(t *testing.T) {
	cfg := testConfig(t, ModeLossless)
	cfg.BlockMaxSamples = 10
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ref := &reference{m: map[int64]float64{}}
	for i := 0; i < 100; i++ {
		s.Write(int64(i), float64(i*i))
		ref.write(int64(i), float64(i*i))
	}
	// 在每个块边界附近做查询。
	for b := int64(0); b <= 100; b += 10 {
		for _, r := range [][2]int64{{b - 1, b + 1}, {b, b}, {b - 5, b + 5}} {
			assertQueryEqual(t, s, ref, r[0], r[1], false, 0)
		}
	}
	// 迟到样本落在已封块范围内。
	s.Write(5, 999)
	ref.write(5, 999)
	s.Write(50, 888)
	ref.write(50, 888)
	assertQueryEqual(t, s, ref, 0, 99, false, 0)
	assertQueryEqual(t, s, ref, 5, 5, false, 0)
}

func ExampleStore() {
	dir, _ := os.MkdirTemp("", "codec-example")
	defer os.RemoveAll(dir)
	cfg := DefaultConfig(dir)
	s, _ := Open(cfg)
	defer s.Close()
	for i := 0; i < 5; i++ {
		s.Write(int64(i*1000), float64(i)*1.5)
	}
	s.Write(2000, 99) // 重复点：后写覆盖
	got, _ := s.Query(0, 4000)
	for _, smp := range got {
		fmt.Printf("t=%d v=%.1f\n", smp.T, smp.V)
	}
	// Output:
	// t=0 v=0.0
	// t=1000 v=1.5
	// t=2000 v=99.0
	// t=3000 v=4.5
	// t=4000 v=6.0
}

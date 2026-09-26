package store

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func TestCompactMergesBlocksAndReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 32
	cfg.MinMergeBlocks = 3
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	model := newReplayModel()
	rng := rand.New(rand.NewSource(5))

	// 320 unique points, written twice (second pass overwrites with new
	// values) in 20 batches -> 20 flushed blocks with heavy overlap.
	for pass := 0; pass < 2; pass++ {
		for b := 0; b < 10; b++ {
			var batch []codec.Point
			for i := 0; i < 32; i++ {
				p := codec.Point{TS: int64(b*32 + i), V: rng.Float64()}
				batch = append(batch, p)
			}
			if err := s.Write("m", batch...); err != nil {
				t.Fatal(err)
			}
			model.write("m", batch...)
		}
		s.Flush()
	}
	before := s.Stats()
	if before.Blocks != 20 {
		t.Fatalf("expected 20 blocks, got %d", before.Blocks)
	}
	want := model.query("m", 0, 1<<60)

	if err := s.CompactOnce(); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if after.Blocks >= before.Blocks {
		t.Fatalf("compaction did not reduce blocks: %d -> %d", before.Blocks, after.Blocks)
	}
	t.Logf("blocks %d -> %d, data files %d -> %d", before.Blocks, after.Blocks, before.DataFiles, after.DataFiles)

	got, err := s.Query("m", 0, 1<<60)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(got, want) {
		t.Fatalf("query result changed by compaction: got %d, want %d", len(got), len(want))
	}

	// unreferenced data files must be gone from disk
	ents, _ := os.ReadDir(dir)
	live := map[string]bool{}
	for _, e := range ents {
		live[e.Name()] = true
	}
	for id := range s.data {
		if !live[dataFileName(id)] {
			t.Fatalf("live data file %s missing on disk", dataFileName(id))
		}
	}
	s.Close()

	// after reopen, still consistent
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ = s2.Query("m", 0, 1<<60)
	if !pointsEqual(got, want) {
		t.Fatal("query result changed after reopen")
	}
}

// TestCompactConcurrentReadWrite: queries and writes running during
// compaction must never see intermediate state or lose committed data.
func TestCompactConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 64
	cfg.MinMergeBlocks = 2
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	model := newReplayModel()
	var mu sync.Mutex // guards model
	rng := rand.New(rand.NewSource(2))
	ts := int64(0)
	for i := 0; i < 400; i++ {
		ts += 10
		p := codec.Point{TS: ts, V: rng.Float64()}
		if err := s.Write("m", p); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		model.write("m", p)
		mu.Unlock()
	}
	s.Flush()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 16)

	// writer goroutine: keeps writing new points
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(21))
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			ts += 10
			p := codec.Point{TS: ts, V: r.Float64()}
			mu.Unlock()
			if err := s.Write("m", p); err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			model.write("m", p)
			mu.Unlock()
		}
	}()

	// reader goroutines: full-range queries must always be sorted and
	// duplicate-free
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pts, err := s.Query("m", 0, 1<<60)
				if err != nil {
					errCh <- err
					return
				}
				for i := 1; i < len(pts); i++ {
					if pts[i].TS <= pts[i-1].TS {
						errCh <- errSorted
						return
					}
				}
			}
		}()
	}

	// compactor
	for i := 0; i < 20; i++ {
		if err := s.CompactOnce(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}

	// final consistency check against the model
	s.Flush()
	got, err := s.Query("m", 0, 1<<60)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	want := model.query("m", 0, 1<<60)
	mu.Unlock()
	if !pointsEqual(got, want) {
		t.Fatalf("final state diverged: got %d points, want %d", len(got), len(want))
	}
}

var errSorted = errorString("query result not strictly sorted")

type errorString string

func (e errorString) Error() string { return string(e) }

// TestCompactNeverDegrades: compaction must not re-encode lossless
// blocks with a lossy codec even if the current config is lossy.
func TestCompactNeverDegrades(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 16
	cfg.MinMergeBlocks = 2
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var exact []codec.Point
	for i := 0; i < 64; i++ {
		p := codec.Point{TS: int64(i), V: float64(i) * 0.123456789}
		exact = append(exact, p)
	}
	s.Write("m", exact...)
	s.Flush()

	// change config to lossy, compact, and verify old data stays exact
	s.cfg.Codec = codec.Config{Codec: codec.CodecLossy, Epsilon: 0.5}
	if err := s.CompactOnce(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Query("m", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(got, exact) {
		t.Fatal("lossless data degraded by compaction under lossy config")
	}
	// verify the merged block is still encoded lossless
	for _, bm := range s.series["m"].blocks {
		raw, _ := s.readBlock(bm)
		inf, _ := codec.BlockInfo(raw)
		if inf.Codec != codec.CodecLossless {
			t.Fatalf("block re-encoded as %v", inf.Codec)
		}
	}
	s.Close()
}

// TestBackgroundCompactLoop: the interval-driven loop runs on its own.
func TestBackgroundCompactLoop(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 8
	cfg.MinMergeBlocks = 2
	cfg.CompactInterval = 20_000_000 // 20ms
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// 10 partial flushes -> up to 10 small blocks of 3 points each (the
	// background loop may already merge some while we write)
	for b := 0; b < 10; b++ {
		for i := 0; i < 3; i++ {
			s.Write("m", codec.Point{TS: int64(b*3 + i), V: float64(b*3 + i)})
		}
		s.Flush()
	}
	// background loop (20ms interval) should merge the small blocks into
	// at most ceil(30/8) = 4 blocks
	deadline := 0
	for s.Stats().Blocks > 4 && deadline < 500 {
		deadline++
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.Stats().Blocks; got > 4 {
		t.Fatalf("background compaction did not merge: %d blocks", got)
	}
	got, _ := s.Query("m", 0, 100)
	if len(got) != 30 {
		t.Fatalf("got %d points, want 30", len(got))
	}
}

// TestCloseWithActiveCompactor: closing while the background compactor
// is firing must not deadlock (regression test).
func TestCloseWithActiveCompactor(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 8
	cfg.MinMergeBlocks = 2
	cfg.CompactInterval = 1 // 1ns: compactor fires as fast as possible
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		s.Write("m", codec.Point{TS: int64(i), V: float64(i)})
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close deadlocked with active compactor")
	}
	got, err := func() ([]codec.Point, error) {
		s2, err := Open(testConfig(dir))
		if err != nil {
			return nil, err
		}
		defer s2.Close()
		return s2.Query("m", 0, 200)
	}()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 200 {
		t.Fatalf("got %d points after close, want 200", len(got))
	}
}

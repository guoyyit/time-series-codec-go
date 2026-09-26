package store

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// TestCrashUnflushedWrites: writes that were never flushed (WAL only)
// must survive a crash (process dies without Close).
func TestCrashUnflushedWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 10000 // nothing flushes automatically

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	model := newReplayModel()
	for i := 0; i < 50; i++ {
		p := codec.Point{TS: int64(i * 10), V: float64(i) * 1.5}
		if err := s.Write("m", p); err != nil {
			t.Fatal(err)
		}
		model.write("m", p)
	}
	// crash: no Close, abandon the store
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Query("m", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !pointsEqual(got, model.query("m", 0, 500)) {
		t.Fatalf("got %d points, want %d", len(got), len(model.query("m", 0, 500)))
	}
}

// TestCrashTornWALTail: a torn WAL record at the tail (crash mid-write)
// is truncated; earlier records survive.
func TestCrashTornWALTail(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 10000

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		s.Write("m", codec.Point{TS: int64(i), V: float64(i)})
	}
	// crash mid-append: garbage bytes at the WAL tail
	gens, err := listWALGens(dir)
	if err != nil || len(gens) != 1 {
		t.Fatalf("gens %v err %v", gens, err)
	}
	walPath := filepath.Join(walDir(dir), walFileName(gens[0]))
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01})
	f.Close()

	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ := s2.Query("m", 0, 100)
	if len(got) != 10 {
		t.Fatalf("got %d points, want 10", len(got))
	}
	// store must still accept writes after truncation
	if err := s2.Write("m", codec.Point{TS: 100, V: 1}); err != nil {
		t.Fatal(err)
	}
	got, _ = s2.Query("m", 0, 200)
	if len(got) != 11 {
		t.Fatalf("got %d points after rewrite, want 11", len(got))
	}
}

// TestCrashStaleIndexTmp: a leftover index.tmp from an interrupted
// persist is ignored and removed.
func TestCrashStaleIndexTmp(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.Write("m", codec.Point{TS: 1, V: 1})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// simulate interrupted persist: garbage tmp file
	if err := os.WriteFile(filepath.Join(dir, indexTmpName), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ := s2.Query("m", 0, 10)
	if len(got) != 1 {
		t.Fatalf("got %d points, want 1", len(got))
	}
	if _, err := os.Stat(filepath.Join(dir, indexTmpName)); !os.IsNotExist(err) {
		t.Fatal("stale index.tmp not cleaned")
	}
}

// TestCorruptIndexFails: a corrupt main index must fail loudly, not
// silently start empty (which would lose all flushed data).
func TestCorruptIndexFails(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	s, _ := Open(cfg)
	s.Write("m", codec.Point{TS: 1, V: 1})
	s.Close()
	p := filepath.Join(dir, indexFileName)
	data, _ := os.ReadFile(p)
	data[10] ^= 0xFF
	os.WriteFile(p, data, 0o644)
	if _, err := Open(cfg); err == nil {
		t.Fatal("expected error opening store with corrupt index")
	}
}

// TestCrashOrphanDataTail: garbage appended to a data file (crash
// between data append and index persist) is ignored.
func TestCrashOrphanDataTail(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		s.Write("m", codec.Point{TS: int64(i), V: float64(i)})
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// crash after a partial block append: garbage at data file tail
	curID := s.curData.id
	s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, dataFileName(curID)), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0xBA, 0xAD, 0xF0, 0x0D})
	f.Close()

	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ := s2.Query("m", 0, 100)
	if len(got) != 100 {
		t.Fatalf("got %d points, want 100", len(got))
	}
}

// TestCrashLoop: randomized crash cycles; the final state must equal a
// full replay of all writes.
func TestCrashLoop(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxBlockPoints = 64
	model := newReplayModel()
	rng := rand.New(rand.NewSource(11))
	ts := int64(0)
	for cycle := 0; cycle < 15; cycle++ {
		s, err := Open(cfg)
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		n := 1 + rng.Intn(200)
		for i := 0; i < n; i++ {
			ts += int64(1 + rng.Intn(10))
			p := codec.Point{TS: ts, V: rng.Float64()}
			if err := s.Write("m", p); err != nil {
				t.Fatalf("cycle %d: %v", cycle, err)
			}
			model.write("m", p)
		}
		if rng.Intn(2) == 0 {
			// clean close sometimes
			if err := s.Close(); err != nil {
				t.Fatalf("cycle %d: %v", cycle, err)
			}
		}
		// else: crash (abandon without Close)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Query("m", 0, ts+1)
	if err != nil {
		t.Fatal(err)
	}
	want := model.query("m", 0, ts+1)
	if !pointsEqual(got, want) {
		t.Fatalf("got %d points, want %d", len(got), len(want))
	}
}

// TestMissingDataFileFails: if a data file referenced by the index is
// lost or truncated, Open must fail loudly (index and data not
// self-consistent) instead of silently recreating an empty file.
func TestMissingDataFileFails(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	s, _ := Open(cfg)
	for i := 0; i < 100; i++ {
		s.Write("m", codec.Point{TS: int64(i), V: float64(i)})
	}
	s.Flush()
	// find the data file holding the block
	var refID uint64
	for _, ss := range s.series {
		for _, bm := range ss.blocks {
			refID = bm.FileID
		}
	}
	s.Close()

	// truncate the referenced data file
	if err := os.Truncate(filepath.Join(dir, dataFileName(refID)), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(cfg); err == nil {
		t.Fatal("expected Open to fail on truncated referenced data file")
	}
}

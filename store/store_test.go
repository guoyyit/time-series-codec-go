package store

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func testConfig(dir string) Config {
	return Config{
		Dir:            dir,
		Codec:          codec.Config{Codec: codec.CodecLossless},
		MaxBlockPoints: 64,
		MinMergeBlocks: 3,
	}
}

// replayModel computes the expected query result by replaying all
// writes with the documented dedup policy (last write wins).
type replayModel struct {
	data map[string]map[int64]float64
}

func newReplayModel() *replayModel { return &replayModel{data: map[string]map[int64]float64{}} }

func (m *replayModel) write(series string, pts ...codec.Point) {
	d, ok := m.data[series]
	if !ok {
		d = map[int64]float64{}
		m.data[series] = d
	}
	for _, p := range pts {
		d[p.TS] = p.V
	}
}

func (m *replayModel) query(series string, start, end int64) []codec.Point {
	d := m.data[series]
	var out []codec.Point
	for ts, v := range d {
		if ts >= start && ts < end {
			out = append(out, codec.Point{TS: ts, V: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	return out
}

func pointsEqual(a, b []codec.Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].TS != b[i].TS || math.Float64bits(a[i].V) != math.Float64bits(b[i].V) {
			return false
		}
	}
	return true
}

func TestWriteQueryFlushReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	model := newReplayModel()
	rng := rand.New(rand.NewSource(3))

	// 300 points, some flushed (block size 64), some in memtable
	var batch []codec.Point
	for i := 0; i < 300; i++ {
		p := codec.Point{TS: int64(i * 1000), V: rng.NormFloat64() * 10}
		batch = append(batch, p)
	}
	if err := s.Write("cpu", batch...); err != nil {
		t.Fatal(err)
	}
	model.write("cpu", batch...)

	check := func(s *Store, what string) {
		t.Helper()
		for _, q := range [][2]int64{{0, 300000}, {50000, 123456}, {64000, 64000}, {299000, 300000}, {100, 200}} {
			got, err := s.Query("cpu", q[0], q[1])
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			want := model.query("cpu", q[0], q[1])
			if !pointsEqual(got, want) {
				t.Fatalf("%s: range %v: got %d points, want %d", what, q, len(got), len(want))
			}
		}
	}
	check(s, "before flush")
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	check(s, "after flush")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: index + blocks must be self-consistent.
	s2, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	check(s2, "after reopen")
}

func TestOutOfOrderAndDuplicates(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	model := newReplayModel()
	rng := rand.New(rand.NewSource(9))

	// shuffled writes with duplicates across several batches, spanning
	// flushes (block size 64)
	var all []codec.Point
	for i := 0; i < 500; i++ {
		all = append(all, codec.Point{TS: int64(i * 100), V: float64(i)})
	}
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	for i := 0; i < len(all); i += 37 {
		end := i + 37
		if end > len(all) {
			end = len(all)
		}
		s.Write("s", all[i:end]...)
		model.write("s", all[i:end]...)
	}
	// duplicate overwrites for a few timestamps (last write wins)
	for i := 0; i < 50; i++ {
		p := codec.Point{TS: int64(rng.Intn(500) * 100), V: -1.5}
		s.Write("s", p)
		model.write("s", p)
	}
	// late sample for an already-flushed range
	late := codec.Point{TS: 5, V: 999}
	s.Write("s", late)
	model.write("s", late)

	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Query("s", 0, 50000)
	if err != nil {
		t.Fatal(err)
	}
	want := model.query("s", 0, 50000)
	if !pointsEqual(got, want) {
		t.Fatalf("got %d points, want %d", len(got), len(want))
	}
	// spot-check overwrite and late sample
	g, _ := s.Query("s", 5, 6)
	if len(g) != 1 || g[0].V != 999 {
		t.Fatalf("late sample wrong: %+v", g)
	}
}

func TestQueryUnknownSeriesAndEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Query("nope", 0, 100)
	if err != nil || got != nil {
		t.Fatalf("got %v %v", got, err)
	}
	s.Write("a", codec.Point{TS: 10, V: 1})
	got, _ = s.Query("a", 0, 10) // half-open: excludes ts=10
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
	got, _ = s.Query("a", 10, 11)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %v", got)
	}
}

func TestMultiSeries(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	model := newReplayModel()
	for _, name := range []string{"a", "b", "c"} {
		for i := 0; i < 100; i++ {
			p := codec.Point{TS: int64(i * 10), V: float64(i) + float64(len(name))}
			s.Write(name, p)
			model.write(name, p)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	names := s2.Series()
	if len(names) != 3 || names[0] != "a" || names[2] != "c" {
		t.Fatalf("series %v", names)
	}
	for _, name := range names {
		got, _ := s2.Query(name, 0, 1000)
		if !pointsEqual(got, model.query(name, 0, 1000)) {
			t.Fatalf("series %s mismatch", name)
		}
	}
}

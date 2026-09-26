package store

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func TestMemtableSortedDedup(t *testing.T) {
	m := newMemtable()
	rng := rand.New(rand.NewSource(1))
	seen := map[int64]float64{}
	// random out-of-order writes with duplicates
	for i := 0; i < 1000; i++ {
		ts := int64(rng.Intn(300))
		v := rng.Float64()
		m.put(codec.Point{TS: ts, V: v})
		seen[ts] = v // last write wins
	}
	if m.len() != len(seen) {
		t.Fatalf("len %d != %d", m.len(), len(seen))
	}
	pts := m.points()
	if !sort.SliceIsSorted(pts, func(i, j int) bool { return pts[i].TS < pts[j].TS }) {
		t.Fatal("not sorted")
	}
	for _, p := range pts {
		if seen[p.TS] != p.V {
			t.Fatalf("ts %d: got %v want %v", p.TS, p.V, seen[p.TS])
		}
		g, ok := m.get(p.TS)
		if !ok || g.V != p.V {
			t.Fatalf("get(%d) failed", p.TS)
		}
	}
	m.reset()
	if m.len() != 0 {
		t.Fatal("reset failed")
	}
}

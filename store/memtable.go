package store

import (
	"sort"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// memtable is a per-series in-memory buffer holding recently written
// points in sorted order. Duplicate timestamps are overwritten
// (last write wins). It is not safe for concurrent use.
//
// Lookups are binary searches; in-order appends are O(log n), random
// inserts additionally memmove the tail.
type memtable struct {
	pts []codec.Point // sorted by TS, unique
}

func newMemtable() *memtable { return &memtable{} }

// put inserts or overwrites a point.
func (m *memtable) put(p codec.Point) {
	i := sort.Search(len(m.pts), func(i int) bool { return m.pts[i].TS >= p.TS })
	if i < len(m.pts) && m.pts[i].TS == p.TS {
		m.pts[i].V = p.V // last write wins
		return
	}
	m.pts = append(m.pts, codec.Point{})
	copy(m.pts[i+1:], m.pts[i:])
	m.pts[i] = p
}

// get returns the point with the given timestamp.
func (m *memtable) get(ts int64) (codec.Point, bool) {
	i := sort.Search(len(m.pts), func(i int) bool { return m.pts[i].TS >= ts })
	if i < len(m.pts) && m.pts[i].TS == ts {
		return m.pts[i], true
	}
	return codec.Point{}, false
}

// len returns the number of buffered points.
func (m *memtable) len() int { return len(m.pts) }

// points returns the sorted buffered points.
func (m *memtable) points() []codec.Point { return m.pts }

// reset clears the buffer.
func (m *memtable) reset() { m.pts = nil }

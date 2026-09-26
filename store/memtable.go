package store

import (
	"sort"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// memtable is a per-series in-memory buffer holding recently written
// points in sorted order. Duplicate timestamps are overwritten
// (last write wins). It is not safe for concurrent use.
type memtable struct {
	pts []codec.Point // sorted by TS
	pos map[int64]int // TS -> index in pts
}

func newMemtable() *memtable {
	return &memtable{pos: make(map[int64]int)}
}

// put inserts or overwrites a point.
func (m *memtable) put(p codec.Point) {
	if i, ok := m.pos[p.TS]; ok {
		m.pts[i].V = p.V // last write wins
		return
	}
	i := sort.Search(len(m.pts), func(i int) bool { return m.pts[i].TS >= p.TS })
	m.pts = append(m.pts, codec.Point{})
	copy(m.pts[i+1:], m.pts[i:])
	m.pts[i] = p
	// indexes at positions >= i shifted by one
	for k, v := range m.pos {
		if v >= i {
			m.pos[k] = v + 1
		}
	}
	m.pos[p.TS] = i
}

// get returns the point with the given timestamp.
func (m *memtable) get(ts int64) (codec.Point, bool) {
	i, ok := m.pos[ts]
	if !ok {
		return codec.Point{}, false
	}
	return m.pts[i], true
}

// len returns the number of buffered points.
func (m *memtable) len() int { return len(m.pts) }

// points returns the sorted buffered points.
func (m *memtable) points() []codec.Point { return m.pts }

// reset clears the buffer.
func (m *memtable) reset() {
	m.pts = nil
	m.pos = make(map[int64]int)
}

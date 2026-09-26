package codec

import "sort"

// memtable 是单个时间序列的内存缓冲，负责乱序/重复样本的
// 排序与去重，并按序产出待封块的样本。
// 语义：同一时间戳后写覆盖先写（last write wins）。
type memtable struct {
	m map[int64]float64
}

func newMemtable() *memtable {
	return &memtable{m: make(map[int64]float64)}
}

// add 插入一个样本；重复时间戳时后写覆盖先写。
func (m *memtable) add(s Sample) {
	m.m[s.T] = s.V
}

// len 返回缓冲样本数。
func (m *memtable) len() int { return len(m.m) }

// sorted 返回全部样本的升序副本。
func (m *memtable) sorted() []Sample {
	out := make([]Sample, 0, len(m.m))
	for t, v := range m.m {
		out = append(out, Sample{T: t, V: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

// drain 按时间升序取出至多 n 个样本并从缓冲中移除。
func (m *memtable) drain(n int) []Sample {
	if n <= 0 || len(m.m) == 0 {
		return nil
	}
	all := m.sorted()
	if len(all) <= n {
		m.m = make(map[int64]float64)
		return all
	}
	out := all[:n]
	for _, s := range out {
		delete(m.m, s.T)
	}
	return out
}

// rangeQuery 返回 [start, end] 内的样本（升序）。
func (m *memtable) rangeQuery(start, end int64) []Sample {
	var out []Sample
	for t, v := range m.m {
		if t >= start && t <= end {
			out = append(out, Sample{T: t, V: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

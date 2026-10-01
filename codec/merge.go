package codec

import (
	"os"
	"path/filepath"
	"sort"
)

// 后台合并：将多个重叠/相邻的小块重压缩为少量大块，回收空间。
//
// 正确性论证：
//   - 块一旦封存即不可变，合并只读旧块、写新块文件；
//   - 索引切换与旧文件删除在同一写锁内完成，查询（读锁）要么看到
//     旧索引+旧文件，要么看到新索引+新文件，绝无中间状态；
//   - 提交前校验合并范围内没有并发封入的新块，否则放弃本次合并，
//     保证"后封块的较新写入覆盖旧块"的裁决规则不被破坏；
//   - 崩溃恢复会清理未被索引引用的孤儿块文件。

// MergeNow 同步触发一次后台合并。没有可合并的块时返回 nil。
func (s *Store) MergeNow() error {
	s.mergeMu.Lock()
	defer s.mergeMu.Unlock()

	// 1. 快照当前索引，挑选合并组。
	s.mu.RLock()
	snap := s.ix.clone()
	s.mu.RUnlock()

	group := pickMergeGroup(snap.blocks, s.cfg.Merge.MinBlocks, 4*s.cfg.BlockMaxSamples)
	if len(group) == 0 {
		return nil
	}
	groupMin, groupMax := group[0].MinT, group[0].MaxT
	for _, b := range group {
		if b.MinT < groupMin {
			groupMin = b.MinT
		}
		if b.MaxT > groupMax {
			groupMax = b.MaxT
		}
	}

	// 2. 无锁解码与合并（块文件不可变，安全）。
	//    按 ID 升序应用，ID 大者覆盖重复时间戳（与查询裁决一致）。
	sort.Slice(group, func(i, j int) bool { return group[i].ID < group[j].ID })
	merged := make(map[int64]float64)
	for _, m := range group {
		data, err := os.ReadFile(filepath.Join(s.cfg.Dir, m.File))
		if err != nil {
			return err
		}
		samples, err := decodeBlock(data)
		if err != nil {
			return err
		}
		for _, smp := range samples {
			merged[smp.T] = smp.V
		}
	}
	all := make([]Sample, 0, len(merged))
	for t, v := range merged {
		all = append(all, Sample{T: t, V: v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].T < all[j].T })

	// 3. 按当前配置重新编码为若干新块（配置变更只影响新块）。
	var chunks [][]Sample
	for i := 0; i < len(all); i += s.cfg.BlockMaxSamples {
		end := i + s.cfg.BlockMaxSamples
		if end > len(all) {
			end = len(all)
		}
		chunks = append(chunks, all[i:end])
	}
	encoded := make([][]byte, len(chunks))
	for i, c := range chunks {
		// 稳定编码：重新量化只有在重建值与输入逐比特相同时才进行，
		// 否则精确保留。这样特殊值（NaN/±Inf）不丢失，有限样本的
		// 误差也不会因反复合并而累积退化，合并前后查询结果一致。
		data, err := encodeBlockOpts(c, s.cfg.Mode, s.cfg.Epsilon, true)
		if err != nil {
			return err
		}
		encoded[i] = data
	}

	// 4. 预分配 ID 并写新块文件（崩溃则成为孤儿，启动时清理）。
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	newIDs := make([]uint64, len(chunks))
	for i := range newIDs {
		newIDs[i] = s.ix.allocID()
	}
	s.mu.Unlock()

	newFiles := make([]string, len(chunks))
	for i, data := range encoded {
		newFiles[i] = blockFileName(newIDs[i])
		if err := s.writeBlockFile(newFiles[i], data); err != nil {
			return err
		}
	}

	// 5. 提交：校验合并范围无并发新块，原子切换索引并删除旧文件。
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	groupIDs := make(map[uint64]bool, len(group))
	for _, b := range group {
		groupIDs[b.ID] = true
	}
	for _, b := range s.ix.overlapping(groupMin, groupMax) {
		if !groupIDs[b.ID] {
			// 合并期间有新块封入该范围：放弃，避免覆盖较新写入。
			for _, f := range newFiles {
				os.Remove(filepath.Join(s.cfg.Dir, f))
			}
			return nil
		}
	}
	next := s.ix.clone()
	next.remove(groupIDs)
	for i, c := range chunks {
		next.add(BlockMeta{
			ID:    newIDs[i],
			File:  newFiles[i],
			MinT:  c[0].T,
			MaxT:  c[len(c)-1].T,
			Count: len(c),
		})
	}
	if err := next.commit(s.cfg.Dir); err != nil {
		return err
	}
	s.ix = next
	for _, b := range group {
		os.Remove(filepath.Join(s.cfg.Dir, b.File))
	}
	return nil
}

// pickMergeGroup 选择合并组。
//
// 块按 MinT 排序后划分为"重叠闭包"分量（分量内任意块都与分量
// 时间范围相交，分量之间互不相交）。合并组由连续的完整分量组成，
// 这保证组外不存在与组范围重叠的块——否则新块会以更高 ID 错误
// 覆盖组外块中更新的写入（见 MergeNow 提交校验）。
func pickMergeGroup(blocks []BlockMeta, minBlocks, maxSamples int) []BlockMeta {
	if minBlocks < 2 {
		minBlocks = 2
	}
	sorted := make([]BlockMeta, len(blocks))
	copy(sorted, blocks)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].MinT != sorted[j].MinT {
			return sorted[i].MinT < sorted[j].MinT
		}
		return sorted[i].ID < sorted[j].ID
	})
	// 划分重叠闭包分量。
	var components [][]BlockMeta
	for _, b := range sorted {
		if n := len(components); n > 0 {
			last := components[n-1]
			if b.MinT <= componentMax(last) {
				components[n-1] = append(last, b)
				continue
			}
		}
		components = append(components, []BlockMeta{b})
	}
	// 按顺序累积完整分量，直到满足最小块数且未超样本上限。
	var group []BlockMeta
	total := 0
	for _, comp := range components {
		compSamples := 0
		for _, b := range comp {
			compSamples += b.Count
		}
		if len(group) >= minBlocks && total+compSamples > maxSamples {
			break
		}
		group = append(group, comp...)
		total += compSamples
	}
	if len(group) < minBlocks {
		return nil
	}
	return group
}

func componentMax(comp []BlockMeta) int64 {
	m := comp[0].MaxT
	for _, b := range comp[1:] {
		if b.MaxT > m {
			m = b.MaxT
		}
	}
	return m
}

package store

import (
	"sort"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// CompactOnce runs one background merge pass synchronously: every series
// with at least MinMergeBlocks blocks gets all its blocks decoded,
// deduplicated and re-encoded into fresh blocks of up to MaxBlockPoints
// points using the current codec configuration. The new blocks are
// written to a new data file and the index is swapped atomically, so
// concurrent queries never observe intermediate state and no committed
// data is lost. Data files that become fully unreferenced are deleted
// after the swap.
func (s *Store) CompactOnce() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.compactOnceLocked()
}

// compactOnceLocked is CompactOnce with the write lock already held.
func (s *Store) compactOnceLocked() error {
	// Plan: which series to merge.
	var todo []string
	for name, ss := range s.series {
		if mergePlan(ss.blocks, s.cfg.MinMergeBlocks) {
			todo = append(todo, name)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	sort.Strings(todo)

	// Decode + merge + re-encode each series. All output blocks go into
	// one fresh data file so partially-orphaned old files can be
	// reclaimed once fully unreferenced.
	var out *dataFile
	newBlocks := make(map[string][]BlockMeta, len(todo))
	for _, name := range todo {
		ss := s.series[name]
		merged, err := s.mergeSeriesPoints(ss.blocks)
		if err != nil {
			return err
		}
		if len(merged) == 0 {
			continue
		}
		if out == nil {
			out, err = openDataFile(s.cfg.Dir, s.nextFile)
			if err != nil {
				return err
			}
			s.nextFile++
		}
		// Never silently degrade precision: the merged blocks keep the
		// strongest codec found among the source blocks (lossless wins;
		// otherwise the smallest epsilon). Changing Config.Codec therefore
		// only affects newly written data, never existing blocks.
		outCfg := mergedCodecConfig(ss.blocks, s)
		var metas []BlockMeta
		for start := 0; start < len(merged); start += s.cfg.MaxBlockPoints {
			end := start + s.cfg.MaxBlockPoints
			if end > len(merged) {
				end = len(merged)
			}
			chunk := merged[start:end]
			blk, err := codec.EncodeBlock(outCfg, chunk)
			if err != nil {
				return err
			}
			off, l, err := out.append(blk)
			if err != nil {
				return err
			}
			metas = append(metas, BlockMeta{
				FileID: out.id,
				Offset: off,
				Length: l,
				Seq:    s.nextSeq,
				Count:  len(chunk),
				T0:     chunk[0].TS,
				T1:     chunk[len(chunk)-1].TS,
			})
			s.nextSeq++
		}
		newBlocks[name] = metas
	}
	if out == nil {
		return nil
	}
	if err := out.sync(); err != nil {
		return err
	}

	// Swap the in-memory index, then persist. If the process crashes
	// before the persist, recovery sees the old snapshot and the old
	// blocks (still on disk); the new file is garbage-collected. No
	// committed data is lost either way.
	for name, metas := range newBlocks {
		s.series[name].blocks = metas
	}
	if err := s.persistLocked(s.walFloor); err != nil {
		return err
	}
	s.data[out.id] = out

	// Delete data files that are now fully unreferenced. We hold the
	// write lock, so no query can be reading them.
	return s.gcUnlocked()
}

// mergeSeriesPoints decodes the given blocks and merges their points:
// duplicates are resolved newest-block-wins (higher Seq), result sorted.
func (s *Store) mergeSeriesPoints(blocks []BlockMeta) ([]codec.Point, error) {
	sorted := make([]BlockMeta, len(blocks))
	copy(sorted, blocks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
	acc := make(map[int64]float64)
	for _, bm := range sorted {
		raw, err := s.readBlock(bm)
		if err != nil {
			return nil, err
		}
		pts, err := codec.DecodeBlock(raw)
		if err != nil {
			return nil, err
		}
		for _, p := range pts {
			acc[p.TS] = p.V
		}
	}
	out := make([]codec.Point, 0, len(acc))
	for ts, v := range acc {
		out = append(out, codec.Point{TS: ts, V: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TS < out[j].TS })
	return out, nil
}

// mergedCodecConfig computes the never-degrade codec configuration for
// re-encoding the given blocks: lossless if any source block is
// lossless, otherwise lossy with the smallest source epsilon.
func mergedCodecConfig(blocks []BlockMeta, s *Store) codec.Config {
	best := codec.Config{Codec: codec.CodecLossy, Epsilon: 0}
	first := true
	for _, bm := range blocks {
		raw, err := s.readBlock(bm)
		if err != nil {
			return s.cfg.Codec // fall back to current config
		}
		inf, err := codec.BlockInfo(raw)
		if err != nil {
			return s.cfg.Codec
		}
		switch {
		case inf.Codec == codec.CodecLossless:
			return codec.Config{Codec: codec.CodecLossless}
		case first || inf.Epsilon < best.Epsilon:
			best = codec.Config{Codec: codec.CodecLossy, Epsilon: inf.Epsilon}
		}
		first = false
	}
	if first {
		return s.cfg.Codec
	}
	return best
}

// mergePlan decides whether a series' blocks should be merged. The
// current policy is deliberately simple: merge once a series accumulates
// at least minBlocks blocks (overlapping late-data blocks and small
// flush fragments both count). A production engine would use tiered
// compaction; the swap protocol would be identical.
func mergePlan(blocks []BlockMeta, minBlocks int) (merge bool) {
	return len(blocks) >= minBlocks
}

// gcUnlocked closes and deletes data files that are no longer
// referenced by any block and are not the current append target.
// Caller must hold the write lock.
func (s *Store) gcUnlocked() error {
	live := make(map[uint64]bool)
	live[s.curData.id] = true
	for _, ss := range s.series {
		for _, bm := range ss.blocks {
			live[bm.FileID] = true
		}
	}
	for id, df := range s.data {
		if live[id] {
			continue
		}
		df.close()
		delete(s.data, id)
		if err := removeDataFile(s.cfg.Dir, id); err != nil {
			return err
		}
	}
	return nil
}

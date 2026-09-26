package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// Store is the storage engine. Use Open to create or recover one.
//
// Concurrency: Write, Query, Flush and CompactOnce may be called
// concurrently. Queries hold a read lock for their whole duration and
// block metadata plus block bytes are immutable once published, so a
// concurrent flush or compaction swap never exposes intermediate state.
// Writes are serialized (WAL append + memtable apply is one critical
// section) so the WAL order always matches the memtable apply order.
type Store struct {
	cfg Config

	mu       sync.RWMutex
	series   map[string]*seriesState
	data     map[uint64]*dataFile // open data files by id
	curData  *dataFile            // current append target
	wal      *wal
	walFloor uint64 // durable snapshot's WALGen: replay floor
	nextFile uint64 // next data file id
	nextSeq  uint64 // next block sequence number
	closed   bool

	compactStop chan struct{}
	compactDone chan struct{}
}

// seriesState holds the per-series block index and memtable.
type seriesState struct {
	blocks []BlockMeta // sorted by (T0, Seq)
	mem    *memtable
}

var (
	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("store: closed")
	// ErrCorruptIndex is returned when the persisted index fails its
	// integrity check.
	ErrCorruptIndex = errBadIndex
)

// Open opens (or creates) the store in cfg.Dir and recovers state:
// it loads the index snapshot, removes unreferenced files, replays the
// WAL generations not yet checkpointed, and starts the background
// compaction loop if configured.
func Open(cfg Config) (*Store, error) {
	if cfg.Dir == "" {
		return nil, errors.New("store: Dir is required")
	}
	cfg.setDefaults()
	if err := cfg.Codec.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	snap, err := loadIndex(cfg.Dir)
	if err != nil {
		return nil, err
	}

	s := &Store{
		cfg:      cfg,
		series:   make(map[string]*seriesState),
		data:     make(map[uint64]*dataFile),
		walFloor: snap.WALGen,
		nextFile: snap.NextFileID,
		nextSeq:  snap.NextSeq,
	}
	for name, blocks := range snap.Series {
		s.series[name] = &seriesState{blocks: blocks, mem: newMemtable()}
	}

	// Open all referenced data files.
	referenced := make(map[uint64]bool)
	for _, ss := range s.series {
		for _, bm := range ss.blocks {
			referenced[bm.FileID] = true
		}
	}
	for id := range referenced {
		df, err := openDataFile(cfg.Dir, id)
		if err != nil {
			s.closeFiles()
			return nil, err
		}
		s.data[id] = df
	}

	// Current append target gets a fresh file id.
	cur, err := openDataFile(cfg.Dir, s.nextFile)
	if err != nil {
		s.closeFiles()
		return nil, err
	}
	s.curData = cur
	s.data[cur.id] = cur
	s.nextFile++

	// Garbage-collect unreferenced data files (orphans from crashes or
	// finished compactions) and stale WAL generations.
	if err := s.gcDataFiles(); err != nil {
		s.closeFiles()
		return nil, err
	}
	if err := removeWALs(cfg.Dir, snap.WALGen); err != nil {
		s.closeFiles()
		return nil, err
	}
	// Remove a stale index tmp file from an interrupted persist.
	os.Remove(filepath.Join(cfg.Dir, indexTmpName))

	// Replay un-checkpointed WAL generations into memtables.
	err = replayWALs(cfg.Dir, snap.WALGen, func(series string, pts []codec.Point) error {
		ss := s.seriesOrCreate(series)
		for _, p := range pts {
			ss.mem.put(p)
		}
		return nil
	})
	if err != nil {
		s.closeFiles()
		return nil, err
	}

	// Open the live WAL. Its generation is the replay floor (or 1 on a
	// fresh store); appending to an existing file is fine because its
	// records were just replayed above.
	liveGen := snap.WALGen
	if liveGen < 1 {
		liveGen = 1
	}
	w, err := openWAL(cfg.Dir, liveGen)
	if err != nil {
		s.closeFiles()
		return nil, err
	}
	s.wal = w

	if cfg.CompactInterval > 0 {
		s.compactStop = make(chan struct{})
		s.compactDone = make(chan struct{})
		go s.compactLoop()
	}
	return s, nil
}

func (s *Store) seriesOrCreate(name string) *seriesState {
	ss, ok := s.series[name]
	if !ok {
		ss = &seriesState{mem: newMemtable()}
		s.series[name] = ss
	}
	return ss
}

func (s *Store) closeFiles() {
	if s.wal != nil {
		s.wal.close()
	}
	for _, df := range s.data {
		df.close()
	}
}

// gcDataFiles deletes data files not referenced by the in-memory index
// and not the current append target. Caller must hold no lock or the
// write lock; it is called from Open and from the compaction swap.
func (s *Store) gcDataFiles() error {
	ents, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}
	live := make(map[uint64]bool)
	live[s.curData.id] = true
	for _, ss := range s.series {
		for _, bm := range ss.blocks {
			live[bm.FileID] = true
		}
	}
	for _, e := range ents {
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x.dat", &id); err != nil {
			continue
		}
		if live[id] {
			continue
		}
		if err := os.Remove(filepath.Join(s.cfg.Dir, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Write appends points to a series. Points may arrive out of order and
// may contain duplicate timestamps (last write wins). The batch is
// durable (WAL fsync) before Write returns.
func (s *Store) Write(series string, pts ...codec.Point) error {
	if len(pts) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.wal.appendPut(series, pts); err != nil {
		return err
	}
	ss := s.seriesOrCreate(series)
	for _, p := range pts {
		ss.mem.put(p)
	}
	if ss.mem.len() >= s.cfg.MaxBlockPoints {
		return s.flushLocked()
	}
	return nil
}

// Query returns the points of a series in the half-open range
// [start, end), sorted by timestamp, deduplicated (newest write wins).
// The result equals a full replay of all writes under the same policy.
func (s *Store) Query(series string, start, end int64) ([]codec.Point, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	ss, ok := s.series[series]
	if !ok {
		return nil, nil
	}
	acc := make(map[int64]float64)
	// Apply blocks oldest-first so newer blocks overwrite duplicates.
	cands := make([]BlockMeta, 0, len(ss.blocks))
	for _, bm := range ss.blocks {
		if bm.T1 < start || bm.T0 >= end {
			continue
		}
		cands = append(cands, bm)
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].Seq < cands[j].Seq })
	for _, bm := range cands {
		raw, err := s.readBlock(bm)
		if err != nil {
			return nil, err
		}
		pts, err := codec.DecodeBlock(raw)
		if err != nil {
			return nil, err
		}
		for _, p := range pts {
			if p.TS >= start && p.TS < end {
				acc[p.TS] = p.V
			}
		}
	}
	// Memtable holds the newest data; it wins over blocks.
	for _, p := range ss.mem.points() {
		if p.TS >= start && p.TS < end {
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

// readBlock reads one block payload. Caller must hold at least RLock.
func (s *Store) readBlock(bm BlockMeta) ([]byte, error) {
	df, ok := s.data[bm.FileID]
	if !ok {
		return nil, errBadRecord
	}
	return df.readAt(bm.Offset, bm.Length)
}

// Flush forces all memtables to be encoded into blocks and checkpoints
// the index and WAL.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.flushLocked()
}

// flushLocked encodes every non-empty memtable into a block, appends the
// blocks to the current data file, atomically persists a new index
// snapshot, and rotates the WAL. Crash at any point is safe:
//   - before the snapshot persists, the old WAL still covers the data;
//   - after it persists, the blocks are durable and the old WAL
//     generation is below the replay floor.
//
// Caller must hold the write lock.
func (s *Store) flushLocked() error {
	type pending struct {
		ss   *seriesState
		name string
		meta BlockMeta
	}
	var pend []pending
	for name, ss := range s.series {
		if ss.mem.len() == 0 {
			continue
		}
		pts := ss.mem.points()
		blk, err := codec.EncodeBlock(s.cfg.Codec, pts)
		if err != nil {
			return err
		}
		off, l, err := s.curData.append(blk)
		if err != nil {
			return err
		}
		pend = append(pend, pending{ss, name, BlockMeta{
			FileID: s.curData.id,
			Offset: off,
			Length: l,
			Seq:    s.nextSeq,
			Count:  len(pts),
			T0:     pts[0].TS,
			T1:     pts[len(pts)-1].TS,
		}})
		s.nextSeq++
	}
	if len(pend) == 0 {
		return nil
	}
	if err := s.curData.sync(); err != nil {
		return err
	}
	// Create the next WAL generation before checkpointing so a crash
	// never leaves the replay floor pointing at a missing file.
	newGen := s.wal.gen + 1
	newWAL, err := openWAL(s.cfg.Dir, newGen)
	if err != nil {
		return err
	}
	// Commit block metadata in memory, then persist the snapshot.
	for _, p := range pend {
		p.ss.blocks = insertBlockMeta(p.ss.blocks, p.meta)
	}
	if err := s.persistLocked(newGen); err != nil {
		return err
	}
	// Rotation and cleanup are best-effort after the checkpoint.
	oldWAL := s.wal
	s.wal = newWAL
	oldWAL.close()
	s.walFloor = newGen
	removeWALs(s.cfg.Dir, newGen)
	// Reset flushed memtables only after the checkpoint succeeded.
	for _, p := range pend {
		p.ss.mem.reset()
	}
	return nil
}

// persistLocked writes the current in-memory state as an index snapshot.
func (s *Store) persistLocked(walFloor uint64) error {
	snap := indexSnapshot{
		WALGen:     walFloor,
		NextFileID: s.nextFile,
		NextSeq:    s.nextSeq,
		Series:     make(map[string][]BlockMeta, len(s.series)),
	}
	for name, ss := range s.series {
		if len(ss.blocks) > 0 {
			snap.Series[name] = ss.blocks
		}
	}
	return snap.persist(s.cfg.Dir)
}

// insertBlockMeta inserts bm keeping the slice sorted by (T0, Seq).
func insertBlockMeta(blocks []BlockMeta, bm BlockMeta) []BlockMeta {
	i := sort.Search(len(blocks), func(i int) bool {
		if blocks[i].T0 != bm.T0 {
			return blocks[i].T0 > bm.T0
		}
		return blocks[i].Seq >= bm.Seq
	})
	blocks = append(blocks, BlockMeta{})
	copy(blocks[i+1:], blocks[i:])
	blocks[i] = bm
	return blocks
}

// Series returns the names of all known series, sorted.
func (s *Store) Series() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.series))
	for name := range s.series {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Stats returns engine statistics.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var st Stats
	st.Series = len(s.series)
	st.DataFiles = len(s.data)
	for _, ss := range s.series {
		st.Blocks += len(ss.blocks)
	}
	for _, df := range s.data {
		st.DataBytes += int64(df.offset)
	}
	return st
}

// Close flushes all state and releases resources. It is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stop, done := s.compactStop, s.compactDone
	s.mu.Unlock()
	// Stop the background compactor without holding the lock: its
	// CompactOnce needs the lock too, so waiting here while holding it
	// would deadlock.
	if stop != nil {
		close(stop)
		<-done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.flushLocked()
	s.wal.close()
	for _, df := range s.data {
		df.close()
	}
	return err
}

// compactLoop runs the background compaction ticker.
func (s *Store) compactLoop() {
	defer close(s.compactDone)
	t := time.NewTicker(s.cfg.CompactInterval)
	defer t.Stop()
	for {
		select {
		case <-s.compactStop:
			return
		case <-t.C:
			s.CompactOnce()
		}
	}
}

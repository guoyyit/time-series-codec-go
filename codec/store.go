package codec

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const blocksDirName = "blocks"

// Store 是时序压缩存储引擎，写入路径与查询路径解耦。
//
// 并发模型：写入与查询共用一把读写锁；查询在 RLock 内完成块读取，
// 因此后台合并的索引切换与旧文件删除（在 Lock 内执行）对查询不可见，
// 查询永远不会读到中间状态。
type Store struct {
	cfg       Config
	blocksDir string

	mu     sync.RWMutex
	mem    *memtable
	ix     *index
	w      *wal
	closed bool

	stopCh  chan struct{}
	stopOne sync.Once
	wg      sync.WaitGroup
	mergeMu sync.Mutex // 串行化合并
}

// Open 打开或创建存储引擎，并执行崩溃恢复。
func Open(cfg Config) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	blocksDir := filepath.Join(cfg.Dir, blocksDirName)
	if err := os.MkdirAll(blocksDir, 0o755); err != nil {
		return nil, err
	}

	ix, err := loadIndex(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("load index: %w", err)
	}
	w, replayed, err := openWAL(cfg.Dir, cfg.SyncWrites)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	s := &Store{
		cfg:       cfg,
		blocksDir: blocksDir,
		mem:       newMemtable(),
		ix:        ix,
		w:         w,
		stopCh:    make(chan struct{}),
	}
	for _, smp := range replayed {
		s.mem.add(smp)
	}
	if err := s.recover(); err != nil {
		w.close()
		return nil, err
	}
	if cfg.Merge.IntervalMillis > 0 {
		s.wg.Add(1)
		go s.mergeLoop()
	}
	return s, nil
}

// recover 清理崩溃残留：删除未被索引引用的孤儿块文件与临时文件，
// 并校验索引引用的块文件存在。
func (s *Store) recover() error {
	referenced := make(map[string]bool)
	for _, b := range s.ix.blocks {
		referenced[b.File] = true
		if _, err := os.Stat(filepath.Join(s.cfg.Dir, b.File)); err != nil {
			return fmt.Errorf("codec: index references missing block %s: %w", b.File, err)
		}
	}
	entries, err := os.ReadDir(s.blocksDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		rel := filepath.Join(blocksDirName, e.Name())
		if !referenced[rel] {
			os.Remove(filepath.Join(s.blocksDir, e.Name())) // 孤儿块：崩溃于封块中途
		}
	}
	// 清理索引临时文件残留。
	os.Remove(filepath.Join(s.cfg.Dir, indexFileName+".tmp"))
	return nil
}

// Write 写入一个样本，支持乱序、重复与迟到样本。
// 重复时间戳后写覆盖先写。
func (s *Store) Write(t int64, v float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.w.append(Sample{T: t, V: v}); err != nil {
		return err
	}
	s.mem.add(Sample{T: t, V: v})
	if s.mem.len() >= s.cfg.BlockMaxSamples {
		return s.sealLocked()
	}
	return nil
}

// sealLocked 将内存缓冲全部封块持久化，并重置 WAL。
// 崩溃安全顺序：块文件落盘 -> 索引原子提交 -> WAL 截断。
// 调用者必须持有写锁。
func (s *Store) sealLocked() error {
	for s.mem.len() > 0 {
		samples := s.mem.drain(s.cfg.BlockMaxSamples)
		data, err := encodeBlock(samples, s.cfg.Mode, s.cfg.Epsilon)
		if err != nil {
			return err
		}
		id := s.ix.allocID()
		meta := BlockMeta{
			ID:    id,
			File:  blockFileName(id),
			MinT:  samples[0].T,
			MaxT:  samples[len(samples)-1].T,
			Count: len(samples),
		}
		if err := s.writeBlockFile(meta.File, data); err != nil {
			return err
		}
		s.ix.add(meta)
	}
	if err := s.ix.commit(s.cfg.Dir); err != nil {
		return err
	}
	return s.w.reset()
}

func blockFileName(id uint64) string {
	return filepath.Join(blocksDirName, fmt.Sprintf("%016x.blk", id))
}

// writeBlockFile 原子写入块文件（临时文件 + fsync + rename）。
func (s *Store) writeBlockFile(rel string, data []byte) error {
	abs := filepath.Join(s.cfg.Dir, rel)
	tmp := abs + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, abs)
}

// Query 返回 [start, end] 内的样本（升序、去重），
// 结果与全量重放一致：重复时间戳取最后一次写入的值。
func (s *Store) Query(start, end int64) ([]Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	metas := s.ix.overlapping(start, end)
	// 块按 ID 升序应用，ID 大者（封块更晚）覆盖先者；
	// 内存缓冲最新，最后应用。
	sort.Slice(metas, func(i, j int) bool { return metas[i].ID < metas[j].ID })
	merged := make(map[int64]float64)
	for _, m := range metas {
		data, err := os.ReadFile(filepath.Join(s.cfg.Dir, m.File))
		if err != nil {
			return nil, err
		}
		samples, err := decodeBlock(data)
		if err != nil {
			return nil, err
		}
		for _, smp := range samples {
			if smp.T >= start && smp.T <= end {
				merged[smp.T] = smp.V
			}
		}
	}
	for _, smp := range s.mem.rangeQuery(start, end) {
		merged[smp.T] = smp.V
	}
	out := make([]Sample, 0, len(merged))
	for t, v := range merged {
		out = append(out, Sample{T: t, V: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out, nil
}

// Flush 强制将内存缓冲封块并持久化。
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.mem.len() == 0 {
		return nil
	}
	return s.sealLocked()
}

// Close 关闭存储引擎：停止后台合并，落盘全部缓冲。可重复调用。
func (s *Store) Close() error {
	s.stopOne.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.mem.len() > 0 {
		err = s.sealLocked()
	}
	if cerr := s.w.close(); err == nil {
		err = cerr
	}
	return err
}

// mergeLoop 是后台合并协程。
func (s *Store) mergeLoop() {
	defer s.wg.Done()
	interval := s.cfg.Merge.IntervalMillis
	for {
		select {
		case <-s.stopCh:
			return
		case <-time.After(time.Duration(interval) * time.Millisecond):
			s.MergeNow()
		}
	}
}

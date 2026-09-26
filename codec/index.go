package codec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

const indexFileName = "index.json"

// BlockMeta 是压缩块的持久化元数据。
type BlockMeta struct {
	ID    uint64 `json:"id"`
	File  string `json:"file"`
	MinT  int64  `json:"min_t"`
	MaxT  int64  `json:"max_t"`
	Count int    `json:"count"`
}

// indexFile 是索引的磁盘格式。
type indexFile struct {
	NextID uint64      `json:"next_id"`
	Blocks []BlockMeta `json:"blocks"`
}

// index 维护块元数据集合，支持持久化与原子替换。
// 索引是不可变快照的载体：查询持有的快照在合并期间保持有效。
type index struct {
	nextID uint64
	blocks []BlockMeta // 按 MinT 升序
}

// loadIndex 从目录加载索引；不存在时返回空索引。
func loadIndex(dir string) (*index, error) {
	path := filepath.Join(dir, indexFileName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &index{nextID: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var f indexFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	ix := &index{nextID: f.NextID, blocks: f.Blocks}
	if ix.nextID == 0 {
		ix.nextID = 1
	}
	ix.sort()
	return ix, nil
}

func (ix *index) sort() {
	sort.Slice(ix.blocks, func(i, j int) bool {
		if ix.blocks[i].MinT != ix.blocks[j].MinT {
			return ix.blocks[i].MinT < ix.blocks[j].MinT
		}
		return ix.blocks[i].ID < ix.blocks[j].ID
	})
}

// allocID 分配下一个块 ID。
func (ix *index) allocID() uint64 {
	id := ix.nextID
	ix.nextID++
	return id
}

// add 追加一个块元数据。
func (ix *index) add(m BlockMeta) {
	ix.blocks = append(ix.blocks, m)
	ix.sort()
}

// remove 删除指定 ID 的块元数据。
func (ix *index) remove(ids map[uint64]bool) {
	kept := ix.blocks[:0]
	for _, b := range ix.blocks {
		if !ids[b.ID] {
			kept = append(kept, b)
		}
	}
	ix.blocks = kept
}

// overlapping 返回与 [start, end] 有交集的块元数据（按 MinT 排序）。
func (ix *index) overlapping(start, end int64) []BlockMeta {
	var out []BlockMeta
	for _, b := range ix.blocks {
		if b.MinT <= end && b.MaxT >= start {
			out = append(out, b)
		}
	}
	return out
}

// clone 返回索引的深拷贝（用于合并期间构造新快照）。
func (ix *index) clone() *index {
	cp := &index{nextID: ix.nextID, blocks: make([]BlockMeta, len(ix.blocks))}
	copy(cp.blocks, ix.blocks)
	return cp
}

// commit 将索引原子持久化到目录（写临时文件 + rename）。
func (ix *index) commit(dir string) error {
	f := indexFile{NextID: ix.nextID, Blocks: ix.blocks}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, indexFileName+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	// fsync 临时文件，保证 rename 后内容可靠。
	if tf, err := os.OpenFile(tmp, os.O_RDWR, 0o644); err == nil {
		tf.Sync()
		tf.Close()
	}
	if err := os.Rename(tmp, filepath.Join(dir, indexFileName)); err != nil {
		return err
	}
	// fsync 目录，保证 rename 本身持久。
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

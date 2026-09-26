// Package store provides a persistent time series storage engine on top
// of the codec package. It supports out-of-order and duplicate writes,
// arbitrary time-range queries without full replay, crash recovery via a
// write-ahead log plus atomic index snapshots, and background merging of
// historical blocks to reclaim space.
package store

import (
	"time"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// Config controls the storage engine. All fields have sane defaults when
// left zero, except Dir which is required.
type Config struct {
	// Dir is the data directory. It is created if missing.
	Dir string
	// Codec selects the compression strategy for newly written blocks.
	// Existing blocks stay decodable after a config change because every
	// block records its own codec id and epsilon.
	Codec codec.Config
	// MaxBlockPoints is the number of points per series that triggers a
	// memtable flush into a compressed block. Default 4096.
	MaxBlockPoints int
	// CompactInterval enables the background merge loop when > 0.
	// Default 0 (disabled; call CompactOnce manually).
	CompactInterval time.Duration
	// MinMergeBlocks is the minimum number of blocks in a series before
	// the compactor considers merging them. Default 4.
	MinMergeBlocks int
}

func (c *Config) setDefaults() {
	if c.MaxBlockPoints <= 0 {
		c.MaxBlockPoints = 4096
	}
	if c.MinMergeBlocks <= 0 {
		c.MinMergeBlocks = 4
	}
}

// BlockMeta describes one compressed block inside a data file.
type BlockMeta struct {
	FileID uint64 // data file generation
	Offset uint64 // byte offset of the block record inside the file
	Length uint32 // length of the block record payload
	Seq    uint64 // global monotonic sequence; newer blocks win on duplicate ts
	Count  int    // number of points
	T0, T1 int64  // inclusive timestamp range covered by the block
}

// Stats reports engine statistics.
type Stats struct {
	Series    int   // number of known series
	Blocks    int   // total number of persisted blocks
	DataFiles int   // number of live data files
	DataBytes int64 // total bytes of live data files
}

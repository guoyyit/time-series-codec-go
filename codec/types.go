// Package codec 提供时序数据的分块压缩编码、持久化存储与区间查询能力。
package codec

import "errors"

// Sample 表示一个时序采样点。T 为毫秒时间戳，V 为采样值。
type Sample struct {
	T int64
	V float64
}

// Mode 表示编码模式。
type Mode int

const (
	// ModeLossless 无损模式：解码结果与原始点完全一致。
	ModeLossless Mode = iota
	// ModeLossy 有损模式：解码误差不超过配置的 Epsilon。
	ModeLossy
)

// MergeConfig 控制后台合并行为。
type MergeConfig struct {
	// Interval 为后台合并扫描间隔；<=0 表示禁用自动合并。
	IntervalMillis int64
	// MinBlocks 触发一次合并所需的最小重叠/相邻块数量。
	MinBlocks int
}

// Config 为存储引擎配置。
type Config struct {
	// Dir 为数据目录。
	Dir string
	// Mode 编码模式（无损/有损）。
	Mode Mode
	// Epsilon 有损模式误差上界（|decoded-original| <= Epsilon）。
	Epsilon float64
	// BlockMaxSamples 每个压缩块的最大样本数，达到后封块。
	BlockMaxSamples int
	// Merge 后台合并配置。
	Merge MergeConfig
	// SyncWrites 为 true 时每次写入都 fsync WAL（最高持久性）；
	// 为 false 时依赖操作系统刷盘，吞吐更高。
	SyncWrites bool
}

// DefaultConfig 返回默认配置。
func DefaultConfig(dir string) Config {
	return Config{
		Dir:             dir,
		Mode:            ModeLossless,
		Epsilon:         0,
		BlockMaxSamples: 1024,
		Merge:           MergeConfig{IntervalMillis: 0, MinBlocks: 2},
		SyncWrites:      true,
	}
}

func (c Config) validate() error {
	if c.Dir == "" {
		return errors.New("codec: Dir is required")
	}
	if c.Mode == ModeLossy && c.Epsilon <= 0 {
		return errors.New("codec: lossy mode requires Epsilon > 0")
	}
	if c.BlockMaxSamples <= 0 {
		return errors.New("codec: BlockMaxSamples must be > 0")
	}
	return nil
}

// ErrClosed 表示存储已关闭。
var ErrClosed = errors.New("codec: store closed")

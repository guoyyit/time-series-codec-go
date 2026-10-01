// Package codec 提供时序数据的分块压缩编码、持久化存储与区间查询能力。
package codec

import (
	"errors"
	"fmt"
	"math"
)

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

var (
	// ErrClosed 表示存储已关闭。
	ErrClosed = errors.New("codec: store closed")
	// ErrInvalidConfig 表示存储配置非法（调用者可用 errors.Is 区分）。
	ErrInvalidConfig = errors.New("codec: invalid config")
	// ErrInvalidEpsilon 表示有损模式的误差上界非法：
	// Epsilon 必须是正的有限数，0、负数、NaN 与正负无穷都会被拒绝。
	ErrInvalidEpsilon = fmt.Errorf("%w: invalid epsilon", ErrInvalidConfig)
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
	// Epsilon 有损模式误差上界（|decoded-original| <= Epsilon），
	// 必须为正的有限数；0、负数、NaN、无穷均为非法配置。
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
		return fmt.Errorf("%w: Dir is required", ErrInvalidConfig)
	}
	if c.Mode != ModeLossless && c.Mode != ModeLossy {
		return fmt.Errorf("%w: unknown mode %d", ErrInvalidConfig, c.Mode)
	}
	if c.Mode == ModeLossy {
		if math.IsNaN(c.Epsilon) {
			return fmt.Errorf("%w: Epsilon must be a positive finite number, got NaN", ErrInvalidEpsilon)
		}
		if math.IsInf(c.Epsilon, 0) {
			sign := "positive"
			if c.Epsilon < 0 {
				sign = "negative"
			}
			return fmt.Errorf("%w: Epsilon must be a positive finite number, got %s infinity", ErrInvalidEpsilon, sign)
		}
		if c.Epsilon <= 0 {
			return fmt.Errorf("%w: Epsilon must be a positive finite number, got %v", ErrInvalidEpsilon, c.Epsilon)
		}
	}
	if c.BlockMaxSamples <= 0 {
		return fmt.Errorf("%w: BlockMaxSamples must be > 0", ErrInvalidConfig)
	}
	return nil
}

// Package codec implements block compression encoding for time series
// data points. It supports a lossless mode (decode result is bit-exact
// identical to the input) and a lossy mode (decode error is bounded by a
// configured epsilon). Every encoded block is self-describing: the block
// header records the codec id and the epsilon that was used, so changing
// configuration never breaks decodability of existing blocks.
package codec

import "errors"

// CodecID identifies the encoding strategy of a block.
type CodecID uint8

const (
	// CodecLossless encodes timestamps with delta-of-delta encoding and
	// values with Gorilla-style XOR encoding. Decoding is bit-exact.
	CodecLossless CodecID = 1
	// CodecLossy quantizes values to multiples of 2*epsilon (max absolute
	// error <= epsilon) and encodes the quantized integers with delta
	// encoding. Timestamps are always lossless.
	CodecLossy CodecID = 2
)

// Point is a single time series sample. TS is a timestamp in nanoseconds
// (the unit is opaque to the codec, it only needs to be monotonic-ish).
type Point struct {
	TS int64
	V  float64
}

// Config selects the encoding strategy for newly written blocks.
type Config struct {
	Codec   CodecID // CodecLossless or CodecLossy
	Epsilon float64 // max absolute error per value, lossy mode only
}

var (
	// ErrInvalidConfig is returned when the codec configuration is invalid
	// (unknown codec id, or non-positive epsilon in lossy mode).
	ErrInvalidConfig = errors.New("codec: invalid config")
	// ErrCorruptBlock is returned when a block fails integrity checks.
	ErrCorruptBlock = errors.New("codec: corrupt block")
	// ErrEmptyInput is returned when encoding zero points.
	ErrEmptyInput = errors.New("codec: empty input")
	// ErrUnsortedInput is returned when input points are not strictly
	// increasing by timestamp.
	ErrUnsortedInput = errors.New("codec: unsorted or duplicate timestamps")
)

// Validate checks the configuration.
func (c Config) Validate() error {
	switch c.Codec {
	case CodecLossless:
		return nil
	case CodecLossy:
		if c.Epsilon <= 0 {
			return ErrInvalidConfig
		}
		return nil
	default:
		return ErrInvalidConfig
	}
}

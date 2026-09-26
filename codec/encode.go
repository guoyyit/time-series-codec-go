package codec

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/bits"
)

// Block binary format (all integers little-endian unless noted):
//
//	Header (32 bytes):
//	  magic    2B  'T','S'
//	  version  1B  currently 1
//	  codec    1B  CodecID
//	  epsilon  8B  float64, max error bound (0 for lossless)
//	  count    4B  uint32 number of points
//	  t0       8B  int64 first timestamp
//	  t1       8B  int64 last timestamp
//	Payload:
//	  timestamps: d0 zigzag-varint (count>=2), then delta-of-delta
//	              bitstream (count>=3)
//	  values:     lossless -> Gorilla XOR bitstream
//	              lossy    -> exact-value side list + zigzag-varint
//	                          deltas of quantized integers
//	Trailer:
//	  crc32    4B  IEEE checksum of all preceding bytes
const (
	blockMagic0  = 'T'
	blockMagic1  = 'S'
	blockVersion = 1
	headerSize   = 32
	crcSize      = 4
)

// EncodeBlock encodes a sorted, deduplicated slice of points into a
// self-contained block. The caller must guarantee pts are sorted by TS
// with no duplicate timestamps.
func EncodeBlock(cfg Config, pts []Point) ([]byte, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(pts) == 0 {
		return nil, ErrEmptyInput
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].TS <= pts[i-1].TS {
			return nil, ErrUnsortedInput
		}
	}

	buf := make([]byte, 0, headerSize+len(pts)*2+crcSize)
	buf = append(buf, blockMagic0, blockMagic1, blockVersion, byte(cfg.Codec))
	eps := cfg.Epsilon
	if cfg.Codec == CodecLossless {
		eps = 0
	}
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(eps))
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(pts)))
	buf = append(buf, tmp[:4]...)
	binary.LittleEndian.PutUint64(tmp[:], uint64(pts[0].TS))
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], uint64(pts[len(pts)-1].TS))
	buf = append(buf, tmp[:]...)

	buf = encodeTimestamps(buf, pts)
	switch cfg.Codec {
	case CodecLossless:
		buf = encodeValuesLossless(buf, pts)
	case CodecLossy:
		buf = encodeValuesLossy(buf, pts, 2*eps)
	}

	crc := crc32.ChecksumIEEE(buf)
	binary.LittleEndian.PutUint32(tmp[:], crc)
	buf = append(buf, tmp[:4]...)
	return buf, nil
}

// encodeTimestamps appends delta-of-delta encoded timestamps.
// All arithmetic wraps (two's complement) consistently with the decoder.
func encodeTimestamps(buf []byte, pts []Point) []byte {
	n := len(pts)
	if n >= 2 {
		buf = appendZigzag(buf, pts[1].TS-pts[0].TS)
	}
	if n >= 3 {
		var bw bitWriter
		prevDelta := pts[1].TS - pts[0].TS
		prev := pts[1].TS
		for i := 2; i < n; i++ {
			delta := pts[i].TS - prev
			dod := delta - prevDelta
			switch {
			case dod == 0:
				bw.writeBit(false)
			case bitRange(dod, 7):
				bw.writeBits(0b10, 2)
				bw.writeBits(uint64(dod), 7)
			case bitRange(dod, 9):
				bw.writeBits(0b110, 3)
				bw.writeBits(uint64(dod), 9)
			case bitRange(dod, 12):
				bw.writeBits(0b1110, 4)
				bw.writeBits(uint64(dod), 12)
			case bitRange(dod, 32):
				bw.writeBits(0b11110, 5)
				bw.writeBits(uint64(dod), 32)
			default:
				bw.writeBits(0b11111, 5)
				bw.writeBits(uint64(dod), 64)
			}
			prevDelta = delta
			prev = pts[i].TS
		}
		buf = append(buf, bw.bytes()...)
	}
	return buf
}

// bitRange reports whether x fits the asymmetric n-bit bucket used by
// Gorilla: -((1<<(n-1))-1) <= x <= 1<<(n-1).
func bitRange(x int64, n uint) bool {
	return x >= -((1<<(n-1))-1) && x <= 1<<(n-1)
}

// encodeValuesLossless appends Gorilla-style XOR encoded values.
func encodeValuesLossless(buf []byte, pts []Point) []byte {
	var bw bitWriter
	prev := math.Float64bits(pts[0].V)
	bw.writeBits(prev, 64)
	prevLeading, prevTrailing := uint(64), uint(0)
	for i := 1; i < len(pts); i++ {
		cur := math.Float64bits(pts[i].V)
		xor := cur ^ prev
		switch {
		case xor == 0:
			bw.writeBit(false)
		default:
			leading := uint(bits.LeadingZeros64(xor))
			trailing := uint(bits.TrailingZeros64(xor))
			if leading >= prevLeading && trailing >= prevTrailing {
				bw.writeBit(true)
				bw.writeBit(false)
				bw.writeBits(xor>>prevTrailing, 64-prevLeading-prevTrailing)
			} else {
				bw.writeBit(true)
				bw.writeBit(true)
				bw.writeBits(uint64(leading), 5)
				sig := 64 - leading - trailing
				bw.writeBits(uint64(sig)&0x3F, 6) // 64 encodes as 0
				bw.writeBits(xor>>trailing, sig)
				prevLeading, prevTrailing = leading, trailing
			}
		}
		prev = cur
	}
	return append(buf, bw.bytes()...)
}

// maxQuantized is the safe |v/step| bound for int64 conversion.
const maxQuantized = 9.0e18

// encodeValuesLossy quantizes finite in-range values to multiples of
// step and delta-encodes them. Non-finite or out-of-range values are
// stored bit-exact in a side list so no information is silently lost.
func encodeValuesLossy(buf []byte, pts []Point, step float64) []byte {
	type exactVal struct {
		idx  int
		bits uint64
	}
	var exacts []exactVal
	qs := make([]int64, 0, len(pts))
	for i, p := range pts {
		q := p.V / step
		if math.IsNaN(q) || math.IsInf(q, 0) || math.Abs(q) > maxQuantized {
			exacts = append(exacts, exactVal{i, math.Float64bits(p.V)})
			continue
		}
		qs = append(qs, int64(math.Round(q)))
	}

	buf = appendUvarint(buf, uint64(len(exacts)))
	prevIdx := 0
	var tmp [8]byte
	for _, e := range exacts {
		buf = appendZigzag(buf, int64(e.idx-prevIdx))
		binary.LittleEndian.PutUint64(tmp[:], e.bits)
		buf = append(buf, tmp[:]...)
		prevIdx = e.idx
	}
	if len(qs) > 0 {
		buf = appendZigzag(buf, qs[0])
		for i := 1; i < len(qs); i++ {
			buf = appendZigzag(buf, qs[i]-qs[i-1])
		}
	}
	return buf
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendZigzag(buf []byte, v int64) []byte {
	ux := uint64(v) << 1
	if v < 0 {
		ux = ^ux
	}
	return appendUvarint(buf, ux)
}

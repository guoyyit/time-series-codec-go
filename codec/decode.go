package codec

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// blockHeader is the parsed block header.
type blockHeader struct {
	codec   CodecID
	epsilon float64
	count   int
	t0, t1  int64
}

// parseHeader validates magic, version and CRC, and returns the header
// plus the payload slice (without header and CRC trailer).
func parseHeader(data []byte) (blockHeader, []byte, error) {
	var h blockHeader
	if len(data) < headerSize+crcSize {
		return h, nil, ErrCorruptBlock
	}
	if data[0] != blockMagic0 || data[1] != blockMagic1 {
		return h, nil, ErrCorruptBlock
	}
	if data[2] != blockVersion {
		return h, nil, ErrCorruptBlock
	}
	h.codec = CodecID(data[3])
	if h.codec != CodecLossless && h.codec != CodecLossy {
		return h, nil, ErrCorruptBlock
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-crcSize:])
	if crc32.ChecksumIEEE(data[:len(data)-crcSize]) != stored {
		return h, nil, ErrCorruptBlock
	}
	h.epsilon = math.Float64frombits(binary.LittleEndian.Uint64(data[4:12]))
	h.count = int(binary.LittleEndian.Uint32(data[12:16]))
	h.t0 = int64(binary.LittleEndian.Uint64(data[16:24]))
	h.t1 = int64(binary.LittleEndian.Uint64(data[24:32]))
	if h.count <= 0 {
		return h, nil, ErrCorruptBlock
	}
	return h, data[headerSize : len(data)-crcSize], nil
}

// DecodeBlock decodes a block previously written by EncodeBlock. The
// codec and epsilon are read from the block header, so blocks remain
// decodable regardless of the current configuration.
func DecodeBlock(data []byte) ([]Point, error) {
	h, payload, err := parseHeader(data)
	if err != nil {
		return nil, err
	}
	ts, rest, err := decodeTimestamps(h, payload)
	if err != nil {
		return nil, err
	}
	vs, err := decodeValues(h, rest)
	if err != nil {
		return nil, err
	}
	pts := make([]Point, h.count)
	for i := range pts {
		pts[i] = Point{TS: ts[i], V: vs[i]}
	}
	return pts, nil
}

// Info describes a block's header fields.
type Info struct {
	Codec   CodecID // encoding strategy recorded in the block
	Epsilon float64 // error bound used (0 for lossless)
	Count   int     // number of points
	T0, T1  int64   // inclusive timestamp range
}

// BlockInfo reads only the block header. It does not verify the CRC
// checksum.
func BlockInfo(data []byte) (Info, error) {
	var inf Info
	if len(data) < headerSize+crcSize {
		return inf, ErrCorruptBlock
	}
	if data[0] != blockMagic0 || data[1] != blockMagic1 || data[2] != blockVersion {
		return inf, ErrCorruptBlock
	}
	inf.Codec = CodecID(data[3])
	inf.Epsilon = math.Float64frombits(binary.LittleEndian.Uint64(data[4:12]))
	inf.Count = int(binary.LittleEndian.Uint32(data[12:16]))
	inf.T0 = int64(binary.LittleEndian.Uint64(data[16:24]))
	inf.T1 = int64(binary.LittleEndian.Uint64(data[24:32]))
	return inf, nil
}

// decodeTimestamps decodes the timestamp stream and returns the
// timestamps plus the remaining payload.
func decodeTimestamps(h blockHeader, payload []byte) ([]int64, []byte, error) {
	n := h.count
	ts := make([]int64, n)
	ts[0] = h.t0
	off := 0
	if n >= 2 {
		d0, m, err := readZigzag(payload[off:])
		if err != nil {
			return nil, nil, err
		}
		off += m
		ts[1] = ts[0] + d0
		if n >= 3 {
			prevDelta := d0
			br := newBitReader(payload[off:])
			for i := 2; i < n; i++ {
				var dod int64
				switch {
				case !br.readBit():
					dod = 0
				case !br.readBit():
					dod = signExtend(br.readBits(7), 7)
				case !br.readBit():
					dod = signExtend(br.readBits(9), 9)
				case !br.readBit():
					dod = signExtend(br.readBits(12), 12)
				case !br.readBit():
					dod = signExtend(br.readBits(32), 32)
				default:
					dod = int64(br.readBits(64))
				}
				if br.err != nil {
					return nil, nil, ErrCorruptBlock
				}
				delta := prevDelta + dod
				ts[i] = ts[i-1] + delta
				prevDelta = delta
			}
			off += br.bytesConsumed()
		}
	}
	return ts, payload[off:], nil
}

// signExtend interprets the n-bit value v as signed using the asymmetric
// Gorilla bucket convention (v > 2^(n-1) means negative).
func signExtend(v uint64, n uint) int64 {
	if v > 1<<(n-1) {
		return int64(v - (1 << n))
	}
	return int64(v)
}

// decodeValues decodes the value stream according to the block codec.
func decodeValues(h blockHeader, payload []byte) ([]float64, error) {
	switch h.codec {
	case CodecLossless:
		return decodeValuesLossless(h.count, payload)
	case CodecLossy:
		return decodeValuesLossy(h.count, payload, 2*h.epsilon)
	default:
		return nil, ErrCorruptBlock
	}
}

func decodeValuesLossless(n int, payload []byte) ([]float64, error) {
	br := newBitReader(payload)
	vs := make([]float64, n)
	prev := br.readBits(64)
	vs[0] = math.Float64frombits(prev)
	prevLeading, prevTrailing := uint(64), uint(0)
	for i := 1; i < n; i++ {
		if !br.readBit() {
			vs[i] = math.Float64frombits(prev)
			continue
		}
		if !br.readBit() {
			// reuse previous leading/trailing window
			sig := 64 - prevLeading - prevTrailing
			xor := br.readBits(sig) << prevTrailing
			prev ^= xor
		} else {
			leading := uint(br.readBits(5))
			sig := uint(br.readBits(6))
			if sig == 0 {
				sig = 64
			}
			trailing := 64 - leading - sig
			xor := br.readBits(sig) << trailing
			prevLeading, prevTrailing = leading, trailing
			prev ^= xor
		}
		if br.err != nil {
			return nil, ErrCorruptBlock
		}
		vs[i] = math.Float64frombits(prev)
	}
	return vs, nil
}

func decodeValuesLossy(n int, payload []byte, step float64) ([]float64, error) {
	off := 0
	nExact, m, err := readUvarint(payload[off:])
	if err != nil {
		return nil, err
	}
	off += m
	if nExact > uint64(n) {
		return nil, ErrCorruptBlock
	}
	vs := make([]float64, n)
	isExact := make([]bool, n)
	prevIdx := 0
	for j := uint64(0); j < nExact; j++ {
		d, m, err := readZigzag(payload[off:])
		if err != nil {
			return nil, err
		}
		off += m
		idx := prevIdx + int(d)
		if idx < 0 || idx >= n || off+8 > len(payload) {
			return nil, ErrCorruptBlock
		}
		isExact[idx] = true
		vs[idx] = math.Float64frombits(binary.LittleEndian.Uint64(payload[off:]))
		off += 8
		prevIdx = idx
	}
	nq := n - int(nExact)
	if nq > 0 {
		q, m, err := readZigzag(payload[off:])
		if err != nil {
			return nil, err
		}
		off += m
		qi := 0
		for i := 0; i < n; i++ {
			if isExact[i] {
				continue
			}
			if qi > 0 {
				d, m, err := readZigzag(payload[off:])
				if err != nil {
					return nil, err
				}
				off += m
				q += d
			}
			vs[i] = float64(q) * step
			qi++
		}
	}
	return vs, nil
}

func readUvarint(data []byte) (uint64, int, error) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, ErrCorruptBlock
	}
	return v, n, nil
}

func readZigzag(data []byte) (int64, int, error) {
	ux, n, err := readUvarint(data)
	if err != nil {
		return 0, 0, err
	}
	v := int64(ux >> 1)
	if ux&1 != 0 {
		v = ^v
	}
	return v, n, nil
}

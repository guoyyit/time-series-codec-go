package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/bits"
)

// 块格式（自描述，配置变更不影响旧块解码）：
//
//	[0:2]  magic "TB"
//	[2]    version = 1（无损块、旧有损块）或 2（新有损块）
//	[3]    mode (0=lossless, 1=lossy)
//	[4:12] epsilon (float64, 有损模式量化步长)
//	[12:16] count (uint32)
//	[16:24] minT (int64)
//	[24:32] maxT (int64)
//	[32:]  payload: 时间戳流 || 值流
//	末尾 4 字节: crc32(之前全部字节)
//
// 时间戳流：delta-of-delta + zigzag varint（无损）。
// 值流（无损）：Gorilla 风格 XOR 位流，NaN 与 ±Inf 同样逐比特保留。
// 值流（有损 v1，仅用于解码旧数据）：round(v/epsilon) 的 zigzag varint 增量。
// 值流（有损 v2）：逐样本标签，标签 0 = 量化值（后接 q 增量 zigzag
// varint，首样本相对 q=0）；标签 1 = 精确值（后接原始 float64 的 8 字节
// 小端比特）。NaN/±Inf 及无法在 |d-v| <= epsilon 内重建的有限样本
// 一律走标签 1 逐比特保留。
const (
	blockMagic0   = 'T'
	blockMagic1   = 'B'
	blockVersion  = 1
	blockVersion2 = 2
	blockHeadSize = 32
	blockCRCSize  = 4

	// lossyTagQuant 表示该样本以量化形式存储。
	lossyTagQuant byte = 0
	// lossyTagExact 表示该样本逐比特保留（特殊值或不满足误差上界）。
	lossyTagExact byte = 1
)

var errCorruptBlock = errors.New("codec: corrupt block")

// encodeBlock 将一组按时间排序的样本编码为一个压缩块。
func encodeBlock(samples []Sample, mode Mode, epsilon float64) ([]byte, error) {
	return encodeBlockOpts(samples, mode, epsilon, false)
}

// encodeBlockOpts 是实际编码入口。stable 为 true 时（后台合并重新
// 编码）采用稳定判定：只有重建值与输入值逐比特相同才允许量化，
// 保证合并不退化既有块的保真性（误差不累积放大）。
func encodeBlockOpts(samples []Sample, mode Mode, epsilon float64, stable bool) ([]byte, error) {
	if len(samples) == 0 {
		return nil, errors.New("codec: cannot encode empty block")
	}
	if mode == ModeLossy && !(epsilon > 0 && !math.IsNaN(epsilon) && !math.IsInf(epsilon, 0)) {
		return nil, errors.New("codec: lossy encoding requires a positive finite epsilon")
	}

	buf := make([]byte, blockHeadSize, blockHeadSize+len(samples)*8)
	buf[0] = blockMagic0
	buf[1] = blockMagic1
	version := uint8(blockVersion)
	if mode == ModeLossy {
		version = blockVersion2
	}
	buf[2] = version
	buf[3] = byte(mode)
	binary.LittleEndian.PutUint64(buf[4:12], math.Float64bits(epsilon))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(samples)))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(samples[0].T))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(samples[len(samples)-1].T))

	// 时间戳流：delta-of-delta（t0 存于头部，故从 i=1 开始）。
	var ts []byte
	prevDelta := int64(0)
	for i := 1; i < len(samples); i++ {
		delta := samples[i].T - samples[i-1].T
		ts = appendVarint(ts, zigzag(delta-prevDelta))
		prevDelta = delta
	}
	buf = append(buf, ts...)

	// 值流。
	switch mode {
	case ModeLossless:
		bw := bitWriter{}
		bw.writeBits(math.Float64bits(samples[0].V), 64)
		prev := math.Float64bits(samples[0].V)
		prevLeading, prevTrailing := ^uint8(0), uint8(0)
		for i := 1; i < len(samples); i++ {
			cur := math.Float64bits(samples[i].V)
			xor := cur ^ prev
			if xor == 0 {
				bw.writeBit(0)
			} else {
				bw.writeBit(1)
				leading := uint8(bits.LeadingZeros64(xor))
				trailing := uint8(bits.TrailingZeros64(xor))
				if leading >= 32 {
					leading = 31
				}
				if prevLeading != ^uint8(0) && leading >= prevLeading && trailing >= prevTrailing {
					// 复用前一个窗口。
					bw.writeBit(0)
					sigBits := 64 - prevLeading - prevTrailing
					bw.writeBits(xor>>prevTrailing, uint(sigBits))
				} else {
					bw.writeBit(1)
					bw.writeBits(uint64(leading), 5)
					sigBits := 64 - leading - trailing
					// sigBits==64 用 0 表示（6 位装不下 64）。
					bw.writeBits(uint64(sigBits)&63, 6)
					bw.writeBits(xor>>trailing, uint(sigBits))
					prevLeading, prevTrailing = leading, trailing
				}
			}
			prev = cur
		}
		buf = append(buf, bw.bytes()...)
	case ModeLossy:
		buf = append(buf, encodeLossyValuesV2(samples, epsilon, stable)...)
	}

	crc := crc32.ChecksumIEEE(buf)
	buf = binary.LittleEndian.AppendUint32(buf, crc)
	return buf, nil
}

// lossyQuantize 判断样本能否量化并满足误差承诺。
// 返回量化整数 q、解码重建值 decoded 与是否可行。
// NaN/±Inf 恒不可行；q 超出 int64 范围、重建溢出/为非有限数，
// 或 |decoded-v| > epsilon 时同样不可行，调用方必须逐比特保留。
func lossyQuantize(v, epsilon float64) (q int64, decoded float64, feasible bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, v, false
	}
	qf := math.Round(v / epsilon)
	if math.IsNaN(qf) || math.IsInf(qf, 0) {
		return 0, v, false
	}
	// qf 必须落在 int64 可表示范围。float64(MaxInt64) 向上舍入为
	// 2^63，故上界取严格小于；float64(MinInt64)=-2^63 可精确表示。
	if qf < float64(math.MinInt64) || qf >= float64(math.MaxInt64) {
		return 0, v, false
	}
	q = int64(qf)
	decoded = float64(q) * epsilon
	if math.IsNaN(decoded) || math.IsInf(decoded, 0) {
		return 0, v, false
	}
	if math.Abs(decoded-v) > epsilon {
		return 0, v, false
	}
	return q, decoded, true
}

// encodeLossyValuesV2 编码 v2 有损值流：逐样本标签 + 载荷。
// stable 为 true 时只在重建值与输入逐比特相同时才量化（合并路径）。
func encodeLossyValuesV2(samples []Sample, epsilon float64, stable bool) []byte {
	var out []byte
	prevQ := int64(0)
	for _, s := range samples {
		q, decoded, feasible := lossyQuantize(s.V, epsilon)
		if feasible && stable && math.Float64bits(decoded) != math.Float64bits(s.V) {
			feasible = false
		}
		if feasible {
			out = append(out, lossyTagQuant)
			out = appendVarint(out, zigzag(q-prevQ))
			prevQ = q
		} else {
			out = append(out, lossyTagExact)
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(s.V))
		}
	}
	return out
}

// decodeBlock 将压缩块解码为按时间排序的样本序列。
func decodeBlock(data []byte) ([]Sample, error) {
	if len(data) < blockHeadSize+blockCRCSize {
		return nil, errCorruptBlock
	}
	if data[0] != blockMagic0 || data[1] != blockMagic1 {
		return nil, errCorruptBlock
	}
	if data[2] != blockVersion && data[2] != blockVersion2 {
		return nil, fmt.Errorf("codec: unsupported block version %d", data[2])
	}
	version := int(data[2])
	body, crcBytes := data[:len(data)-blockCRCSize], data[len(data)-blockCRCSize:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(crcBytes) {
		return nil, errCorruptBlock
	}

	mode := Mode(body[3])
	epsilon := math.Float64frombits(binary.LittleEndian.Uint64(body[4:12]))
	count := int(binary.LittleEndian.Uint32(body[12:16]))
	minT := int64(binary.LittleEndian.Uint64(body[16:24]))
	if count <= 0 {
		return nil, errCorruptBlock
	}
	payload := body[blockHeadSize:]

	// 解码时间戳。
	ts := make([]int64, count)
	ts[0] = minT
	rest := payload
	prevDelta := int64(0)
	for i := 1; i < count; i++ {
		u, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, errCorruptBlock
		}
		rest = rest[n:]
		delta := prevDelta + unzigzag(u)
		ts[i] = ts[i-1] + delta
		prevDelta = delta
	}
	return decodeValues(version, mode, epsilon, count, ts, rest)
}

// decodeValues 在消费时间戳流之后解码值流。
func decodeValues(version int, mode Mode, epsilon float64, count int, ts []int64, rest []byte) ([]Sample, error) {
	out := make([]Sample, count)
	for i := range out {
		out[i].T = ts[i]
	}
	switch mode {
	case ModeLossless:
		br := bitReader{data: rest}
		prev := br.readBits(64)
		if br.err {
			return nil, errCorruptBlock
		}
		out[0].V = math.Float64frombits(prev)
		prevLeading, prevTrailing := ^uint8(0), uint8(0)
		for i := 1; i < count; i++ {
			if br.readBit() == 0 {
				out[i].V = out[i-1].V
				continue
			}
			if br.readBit() == 0 {
				// 复用窗口。
				sigBits := 64 - prevLeading - prevTrailing
				x := br.readBits(uint(sigBits)) << prevTrailing
				prev ^= x
			} else {
				leading := uint8(br.readBits(5))
				sigBits := uint8(br.readBits(6))
				if sigBits == 0 {
					sigBits = 64
				}
				trailing := 64 - leading - sigBits
				x := br.readBits(uint(sigBits)) << trailing
				prev ^= x
				prevLeading, prevTrailing = leading, trailing
			}
			if br.err {
				return nil, errCorruptBlock
			}
			out[i].V = math.Float64frombits(prev)
		}
	case ModeLossy:
		if version == blockVersion2 {
			if err := decodeLossyValuesV2(out, epsilon, rest); err != nil {
				return nil, err
			}
			return out, nil
		}
		// v1 旧有损格式：解码规则保持不变。
		prevQ := int64(0)
		for i := 0; i < count; i++ {
			u, n := binary.Uvarint(rest)
			if n <= 0 {
				return nil, errCorruptBlock
			}
			rest = rest[n:]
			prevQ += unzigzag(u)
			out[i].V = float64(prevQ) * epsilon
		}
	default:
		return nil, fmt.Errorf("codec: unknown mode %d", mode)
	}
	return out, nil
}

// decodeLossyValuesV2 解码 v2 有损值流：标签 0 = 量化增量 varint，
// 标签 1 = 后 8 字节原始 float64 比特。
func decodeLossyValuesV2(out []Sample, epsilon float64, rest []byte) error {
	prevQ := int64(0)
	for i := 0; i < len(out); i++ {
		if len(rest) < 1 {
			return errCorruptBlock
		}
		tag := rest[0]
		rest = rest[1:]
		switch tag {
		case lossyTagQuant:
			u, n := binary.Uvarint(rest)
			if n <= 0 {
				return errCorruptBlock
			}
			rest = rest[n:]
			prevQ += unzigzag(u)
			out[i].V = float64(prevQ) * epsilon
		case lossyTagExact:
			if len(rest) < 8 {
				return errCorruptBlock
			}
			out[i].V = math.Float64frombits(binary.LittleEndian.Uint64(rest[:8]))
			rest = rest[8:]
		default:
			return errCorruptBlock
		}
	}
	return nil
}

// --- 编解码辅助 ---

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

func appendVarint(b []byte, u uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], u)
	return append(b, tmp[:n]...)
}

type bitWriter struct {
	buf   []byte
	cur   byte
	nbits uint8
}

func (w *bitWriter) writeBit(b uint8) {
	w.cur <<= 1
	if b != 0 {
		w.cur |= 1
	}
	w.nbits++
	if w.nbits == 8 {
		w.buf = append(w.buf, w.cur)
		w.cur, w.nbits = 0, 0
	}
}

func (w *bitWriter) writeBits(v uint64, n uint) {
	for i := n; i > 0; i-- {
		w.writeBit(uint8((v >> (i - 1)) & 1))
	}
}

func (w *bitWriter) bytes() []byte {
	if w.nbits > 0 {
		w.cur <<= 8 - w.nbits
		w.buf = append(w.buf, w.cur)
		w.cur, w.nbits = 0, 0
	}
	return w.buf
}

type bitReader struct {
	data  []byte
	off   int
	nbits uint8 // 当前字节剩余位数
	err   bool
}

func (r *bitReader) readBit() uint8 {
	if r.off >= len(r.data) {
		r.err = true
		return 0
	}
	b := (r.data[r.off] >> (7 - r.nbits)) & 1
	r.nbits++
	if r.nbits == 8 {
		r.off++
		r.nbits = 0
	}
	return b
}

func (r *bitReader) readBits(n uint) uint64 {
	var v uint64
	for i := uint(0); i < n; i++ {
		v = (v << 1) | uint64(r.readBit())
	}
	return v
}

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
//	[2]    version (1 或 2)
//	[3]    mode (0=lossless, 1=lossy)
//	[4:12] epsilon (float64, 有损模式量化步长)
//	[12:16] count (uint32)
//	[16:24] minT (int64)
//	[24:32] maxT (int64)
//	[32:]  payload: 时间戳流 || 值流
//	末尾 4 字节: crc32(之前全部字节)
//
// 时间戳流：delta-of-delta + zigzag varint（无损）。
// 值流（无损）：Gorilla 风格 XOR 位流。
// 值流（有损 v1，仅用于解码既有数据）：round(v/epsilon) 的 zigzag varint
//
//	增量序列，误差 <= epsilon/2。
//
// 值流（有损 v2）：逐样本一条记录——
//
//	uvarint(0)        ：转义，后跟 8 字节原始 float64 位（NaN/Inf/无法
//	                    满足误差上界的样本逐比特保留）；
//	uvarint(z+1) (z>0)：量化增量 zigzag(delta) = z，解码值 q*epsilon。
//
// v2 编码时逐样本校验 |decoded-original| <= epsilon 且量化表示幂等
// （对解码值再次量化得到同一 q），不满足则转义为原始值，因此任意
// 有限样本的误差上界在封块、重启、合并重编码后都严格成立。
const (
	blockMagic0 = 'T'
	blockMagic1 = 'B'
	// blockVersionV1 是既有落盘数据的版本：有损值流无转义机制，
	// 其解码规则不得改变。
	blockVersionV1 = 1
	// blockVersion 是当前写入版本。
	blockVersion  = 2
	blockHeadSize = 32
	blockCRCSize  = 4
)

var errCorruptBlock = errors.New("codec: corrupt block")

// maxInt64Float 是 2^63 的 float64 值；|q| >= 2^63 时 int64 转换越界。
const maxInt64Float = 9223372036854775808.0

// encodeBlock 将一组按时间排序的样本编码为一个压缩块。
func encodeBlock(samples []Sample, mode Mode, epsilon float64) ([]byte, error) {
	if len(samples) == 0 {
		return nil, errors.New("codec: cannot encode empty block")
	}
	if mode == ModeLossy && !validEpsilon(epsilon) {
		return nil, ErrInvalidEpsilon
	}

	buf := make([]byte, blockHeadSize, blockHeadSize+len(samples)*8)
	buf[0] = blockMagic0
	buf[1] = blockMagic1
	buf[2] = blockVersion
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
		prevQ := int64(0)
		for _, s := range samples {
			if q, ok := quantizeLossy(s.V, epsilon); ok {
				// z == MaxUint64 时 z+1 回绕为 0（转义标记），改走转义。
				if z := zigzag(q - prevQ); z != math.MaxUint64 {
					buf = appendVarint(buf, z+1)
					prevQ = q
					continue
				}
			}
			// 转义：逐比特保留原始值（NaN/Inf/无法满足误差上界的样本）。
			buf = appendVarint(buf, 0)
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(s.V))
		}
	}

	crc := crc32.ChecksumIEEE(buf)
	buf = binary.LittleEndian.AppendUint32(buf, crc)
	return buf, nil
}

// quantizeLossy 尝试将 v 量化为 epsilon 的整数倍，返回量化值 q。
// 仅当全部数值保证成立时返回 ok=true：
//   - v 为有限数（NaN 与正负无穷必须转义）；
//   - q 可表示为 int64（|q| < 2^63，避免越界转换的实现定义行为）；
//   - 解码值 float64(q)*epsilon 与 v 的偏差不超过 epsilon（编码时校验，
//     覆盖 float64 乘法舍入与极端量级）；
//   - 量化表示幂等：对解码值再次量化得到同一 q，保证后台合并按相同
//     epsilon 重编码时值逐比特不漂移、误差不累积。
//
// 否则返回 ok=false，调用方必须按原始值逐比特保留（转义）。
func quantizeLossy(v, epsilon float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	q := math.Round(v / epsilon)
	if !(math.Abs(q) < maxInt64Float) {
		return 0, false
	}
	qi := int64(q)
	dec := float64(qi) * epsilon
	if !(math.Abs(dec-v) <= epsilon) {
		return 0, false
	}
	// 幂等性在 float64 域比较，避免再次越界转换。
	if math.Round(dec/epsilon) != q {
		return 0, false
	}
	return qi, true
}

// decodeBlock 将压缩块解码为按时间排序的样本序列。
func decodeBlock(data []byte) ([]Sample, error) {
	if len(data) < blockHeadSize+blockCRCSize {
		return nil, errCorruptBlock
	}
	if data[0] != blockMagic0 || data[1] != blockMagic1 {
		return nil, errCorruptBlock
	}
	version := int(data[2])
	if version != blockVersionV1 && version != blockVersion {
		return nil, fmt.Errorf("codec: unsupported block version %d", version)
	}
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
	return decodeValues(mode, epsilon, version, count, ts, rest)
}

// decodeValues 在消费时间戳流之后解码值流。
func decodeValues(mode Mode, epsilon float64, version int, count int, ts []int64, rest []byte) ([]Sample, error) {
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
		prevQ := int64(0)
		for i := 0; i < count; i++ {
			u, n := binary.Uvarint(rest)
			if n <= 0 {
				return nil, errCorruptBlock
			}
			rest = rest[n:]
			if version >= 2 {
				if u == 0 {
					// 转义：8 字节原始 float64 位。
					if len(rest) < 8 {
						return nil, errCorruptBlock
					}
					out[i].V = math.Float64frombits(binary.LittleEndian.Uint64(rest[:8]))
					rest = rest[8:]
					continue
				}
				u--
			}
			prevQ += unzigzag(u)
			out[i].V = float64(prevQ) * epsilon
		}
	default:
		return nil, fmt.Errorf("codec: unknown mode %d", mode)
	}
	return out, nil
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

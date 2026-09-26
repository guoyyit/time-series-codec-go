package codec

import "errors"

// bitWriter appends bits MSB-first into a byte slice.
type bitWriter struct {
	buf   []byte
	nbits uint // number of bits used in the last partial byte (0 means aligned)
}

var errBitstreamEOF = errors.New("codec: bitstream exhausted")

func (w *bitWriter) writeBit(b bool) {
	if w.nbits == 0 {
		w.buf = append(w.buf, 0)
	}
	if b {
		w.buf[len(w.buf)-1] |= 1 << (7 - w.nbits)
	}
	w.nbits = (w.nbits + 1) % 8
}

// writeBits writes the n lowest bits of v, MSB first. n must be <= 64.
func (w *bitWriter) writeBits(v uint64, n uint) {
	for i := n; i > 0; i-- {
		w.writeBit(v&(1<<(i-1)) != 0)
	}
}

// bytes returns the written bytes. The last byte is zero-padded.
func (w *bitWriter) bytes() []byte { return w.buf }

// bitReader reads bits MSB-first from a byte slice.
type bitReader struct {
	data []byte
	pos  int  // byte position
	off  uint // bit offset within current byte
	err  error
}

func newBitReader(data []byte) *bitReader { return &bitReader{data: data} }

func (r *bitReader) readBit() bool {
	if r.err != nil {
		return false
	}
	if r.pos >= len(r.data) {
		r.err = errBitstreamEOF
		return false
	}
	b := r.data[r.pos]&(1<<(7-r.off)) != 0
	r.off++
	if r.off == 8 {
		r.off = 0
		r.pos++
	}
	return b
}

// readBits reads n bits (n <= 64) MSB first.
func (r *bitReader) readBits(n uint) uint64 {
	var v uint64
	for i := uint(0); i < n; i++ {
		v <<= 1
		if r.readBit() {
			v |= 1
		}
	}
	return v
}

// bytesConsumed returns the number of input bytes consumed so far,
// rounding up when the reader is mid-byte.
func (r *bitReader) bytesConsumed() int {
	if r.off > 0 {
		return r.pos + 1
	}
	return r.pos
}

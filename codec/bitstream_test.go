package codec

import "testing"

func TestBitstreamRoundTrip(t *testing.T) {
	var w bitWriter
	w.writeBit(true)
	w.writeBit(false)
	w.writeBit(true)
	w.writeBits(0xAB, 8)
	w.writeBits(0x1FFFFFFFFFFFF, 49)
	w.writeBits(0, 3)
	w.writeBits(0xFFFFFFFFFFFFFFFF, 64)

	r := newBitReader(w.bytes())
	if !r.readBit() || r.readBit() || !r.readBit() {
		t.Fatal("bit mismatch")
	}
	if got := r.readBits(8); got != 0xAB {
		t.Fatalf("got %#x", got)
	}
	if got := r.readBits(49); got != 0x1FFFFFFFFFFFF {
		t.Fatalf("got %#x", got)
	}
	if got := r.readBits(3); got != 0 {
		t.Fatalf("got %#x", got)
	}
	if got := r.readBits(64); got != 0xFFFFFFFFFFFFFFFF {
		t.Fatalf("got %#x", got)
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	// Reading past the end (beyond zero padding) must set err, not panic.
	for i := 0; i < 9 && r.err == nil; i++ {
		r.readBit()
	}
	if r.err == nil {
		t.Fatal("expected EOF error")
	}
}

package store

import (
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func TestClosedStoreOps(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	s.Write("m", codec.Point{TS: 1, V: 1})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// double close is a no-op
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Write("m", codec.Point{TS: 2, V: 2}); err != ErrClosed {
		t.Fatalf("write on closed: %v", err)
	}
	if _, err := s.Query("m", 0, 10); err != ErrClosed {
		t.Fatalf("query on closed: %v", err)
	}
	if err := s.Flush(); err != ErrClosed {
		t.Fatalf("flush on closed: %v", err)
	}
	if err := s.CompactOnce(); err != ErrClosed {
		t.Fatalf("compact on closed: %v", err)
	}
}

func TestEmptyWriteAndInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Write("m"); err != nil {
		t.Fatalf("empty write: %v", err)
	}

	bad := testConfig(dir + "/bad")
	bad.Codec = codec.Config{Codec: codec.CodecLossy, Epsilon: 0}
	if _, err := Open(bad); err == nil {
		t.Fatal("expected error for zero epsilon lossy config")
	}
	bad2 := testConfig(dir + "/bad2")
	bad2.Codec = codec.Config{Codec: 42}
	if _, err := Open(bad2); err == nil {
		t.Fatal("expected error for unknown codec")
	}
}

func TestDefaultsApplied(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, Codec: codec.Config{Codec: codec.CodecLossless}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.cfg.MaxBlockPoints != 4096 || s.cfg.MinMergeBlocks != 4 {
		t.Fatalf("defaults not applied: %+v", s.cfg)
	}
}

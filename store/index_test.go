package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	snap := emptySnapshot()
	snap.WALGen = 7
	snap.NextFileID = 3
	snap.NextSeq = 42
	snap.Series["cpu"] = []BlockMeta{
		{FileID: 1, Offset: 0, Length: 100, Seq: 1, Count: 10, T0: 1000, T1: 2000},
		{FileID: 1, Offset: 106, Length: 90, Seq: 5, Count: 8, T0: 1500, T1: 3000}, // overlapping (late data)
		{FileID: 2, Offset: 0, Length: 80, Seq: 9, Count: 5, T0: 3001, T1: 4000},
	}
	snap.Series["mem"] = []BlockMeta{
		{FileID: 2, Offset: 86, Length: 50, Seq: 10, Count: 3, T0: -500, T1: 100},
	}
	if err := snap.persist(dir); err != nil {
		t.Fatal(err)
	}
	got, err := loadIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.WALGen != 7 || got.NextFileID != 3 || got.NextSeq != 42 {
		t.Fatalf("bad counters: %+v", got)
	}
	for name, want := range snap.Series {
		bs := got.Series[name]
		if len(bs) != len(want) {
			t.Fatalf("series %s: %d blocks != %d", name, len(bs), len(want))
		}
		for i := range want {
			if bs[i] != want[i] {
				t.Fatalf("series %s block %d: got %+v want %+v", name, i, bs[i], want[i])
			}
		}
	}
}

func TestIndexCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	snap := emptySnapshot()
	if err := snap.persist(dir); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, indexFileName)
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 0xFF
	os.WriteFile(p, data, 0o644)
	if _, err := loadIndex(dir); err == nil {
		t.Fatal("expected error for corrupted index")
	}
}

func TestIndexMissingIsEmpty(t *testing.T) {
	dir := t.TempDir()
	snap, err := loadIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Series) != 0 || snap.NextFileID != 1 {
		t.Fatalf("expected empty snapshot, got %+v", snap)
	}
}

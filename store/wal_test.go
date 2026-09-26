package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/time-series-codec-go/codec"
)

func TestWALReplayAndTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := openWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.appendPut("cpu", []codec.Point{{TS: 1, V: 1.5}, {TS: 2, V: 2.5}}); err != nil {
		t.Fatal(err)
	}
	if err := w.appendPut("mem", []codec.Point{{TS: 9, V: -3}}); err != nil {
		t.Fatal(err)
	}
	w.close()

	// append garbage to simulate a torn tail
	p := filepath.Join(walDir(dir), walFileName(1))
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0x01, 0x02, 0x03})
	f.Close()

	var got []codec.Point
	var series []string
	err = replayWALs(dir, 0, func(s string, pts []codec.Point) error {
		series = append(series, s)
		got = append(got, pts...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || series[0] != "cpu" || series[1] != "mem" {
		t.Fatalf("bad replay: %v %v", series, got)
	}
	if got[0].TS != 1 || got[0].V != 1.5 || got[2].V != -3 {
		t.Fatalf("bad points: %+v", got)
	}
	// torn tail must have been truncated
	fi, _ := os.Stat(p)
	w2, _ := openWAL(dir, 1)
	w2.appendPut("x", []codec.Point{{TS: 5, V: 5}})
	w2.close()
	fi2, _ := os.Stat(p)
	if fi2.Size() <= fi.Size() {
		t.Fatal("append after truncation failed")
	}
	var n int
	replayWALs(dir, 0, func(s string, pts []codec.Point) error { n += len(pts); return nil })
	if n != 4 {
		t.Fatalf("expected 4 points after re-append, got %d", n)
	}
}

func TestRemoveWALs(t *testing.T) {
	dir := t.TempDir()
	for g := uint64(1); g <= 3; g++ {
		w, _ := openWAL(dir, g)
		w.appendPut("s", []codec.Point{{TS: 1, V: 1}})
		w.close()
	}
	if err := removeWALs(dir, 3); err != nil {
		t.Fatal(err)
	}
	gens, _ := listWALGens(dir)
	if len(gens) != 1 || gens[0] != 3 {
		t.Fatalf("gens %v", gens)
	}
}

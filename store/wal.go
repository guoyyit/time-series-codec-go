package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/guoyyit/time-series-codec-go/codec"
)

// wal is a write-ahead log. Every write batch is appended (and fsynced)
// before it is applied to the in-memory state, so a crash replays it.
// Record layout: [crc32 4B][payloadLen 4B LE][payload]. A torn tail
// record (bad crc or short read) stops replay and is truncated.
//
// Payload layout for a put record:
//
//	[op 1B = walOpPut][seriesLen uvarint][series][count uvarint]
//	[count * (ts 8B LE)(value bits 8B LE)]
type wal struct {
	f   *os.File
	gen uint64
	dir string
}

const walOpPut = 1

func walDir(dir string) string { return filepath.Join(dir, "wal") }

func walFileName(gen uint64) string { return fmt.Sprintf("%016x.log", gen) }

// openWAL opens (or creates) the WAL file for the given generation,
// positioned for appending.
func openWAL(dir string, gen uint64) (*wal, error) {
	if err := os.MkdirAll(walDir(dir), 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(walDir(dir), walFileName(gen))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, gen: gen, dir: dir}, nil
}

// appendPut appends a put record for one series and fsyncs.
func (w *wal) appendPut(series string, pts []codec.Point) error {
	payload := make([]byte, 0, 1+len(series)+10+len(pts)*16)
	payload = append(payload, walOpPut)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(series)))
	payload = append(payload, tmp[:n]...)
	payload = append(payload, series...)
	n = binary.PutUvarint(tmp[:], uint64(len(pts)))
	payload = append(payload, tmp[:n]...)
	var b [8]byte
	for _, p := range pts {
		binary.LittleEndian.PutUint64(b[:], uint64(p.TS))
		payload = append(payload, b[:]...)
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(p.V))
		payload = append(payload, b[:]...)
	}
	rec := make([]byte, 0, 8+len(payload))
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(b[:4], crc)
	rec = append(rec, b[:4]...)
	binary.LittleEndian.PutUint32(b[:4], uint32(len(payload)))
	rec = append(rec, b[:4]...)
	rec = append(rec, payload...)
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	return w.f.Sync()
}

// close closes the WAL file.
func (w *wal) close() error { return w.f.Close() }

// listWALGens returns the sorted WAL generations present in dir.
func listWALGens(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(walDir(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var gens []uint64
	for _, e := range ents {
		var gen uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x.log", &gen); err == nil {
			gens = append(gens, gen)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens, nil
}

// replayWALs replays all WAL generations >= minGen in order, invoking
// apply for each record. A torn tail (short read or CRC mismatch, e.g.
// from a crash mid-write) is truncated and replay stops for that file.
func replayWALs(dir string, minGen uint64, apply func(series string, pts []codec.Point) error) error {
	gens, err := listWALGens(dir)
	if err != nil {
		return err
	}
	for _, gen := range gens {
		if gen < minGen {
			continue
		}
		if err := replayOneWAL(dir, gen, apply); err != nil {
			return err
		}
	}
	return nil
}

func replayOneWAL(dir string, gen uint64, apply func(series string, pts []codec.Point) error) error {
	path := filepath.Join(walDir(dir), walFileName(gen))
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	off := 0
	for off+8 <= len(data) {
		crc := binary.LittleEndian.Uint32(data[off:])
		l := int(binary.LittleEndian.Uint32(data[off+4:]))
		if l < 0 || off+8+l > len(data) {
			break // torn tail
		}
		payload := data[off+8 : off+8+l]
		if crc32.ChecksumIEEE(payload) != crc {
			break // corrupted tail
		}
		series, pts, err := decodeWALPayload(payload)
		if err != nil {
			break // undecodable record: treat as torn tail
		}
		if err := apply(series, pts); err != nil {
			return err
		}
		off += 8 + l
	}
	if off < len(data) {
		// truncate torn tail so future appends start clean
		return os.Truncate(path, int64(off))
	}
	return nil
}

func decodeWALPayload(payload []byte) (string, []codec.Point, error) {
	if len(payload) == 0 || payload[0] != walOpPut {
		return "", nil, errBadRecord
	}
	off := 1
	sl, n := binary.Uvarint(payload[off:])
	if n <= 0 || off+n+int(sl) > len(payload) {
		return "", nil, errBadRecord
	}
	off += n
	series := string(payload[off : off+int(sl)])
	off += int(sl)
	cnt, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return "", nil, errBadRecord
	}
	off += n
	if off+int(cnt)*16 > len(payload) {
		return "", nil, errBadRecord
	}
	pts := make([]codec.Point, cnt)
	for i := range pts {
		pts[i].TS = int64(binary.LittleEndian.Uint64(payload[off:]))
		pts[i].V = math.Float64frombits(binary.LittleEndian.Uint64(payload[off+8:]))
		off += 16
	}
	return series, pts, nil
}

// removeWALs deletes WAL generations < keepGen.
func removeWALs(dir string, keepGen uint64) error {
	gens, err := listWALGens(dir)
	if err != nil {
		return err
	}
	for _, gen := range gens {
		if gen < keepGen {
			if err := os.Remove(filepath.Join(walDir(dir), walFileName(gen))); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

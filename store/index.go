package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// indexSnapshot is the durable metadata of the engine: the set of live
// blocks per series, the WAL generation to replay from, and the next
// file/sequence numbers. It is persisted atomically (write tmp file,
// fsync, rename, fsync dir) and protected by a CRC32 checksum.
//
// File layout:
//
//	[magic 3B 'I','D','X'][version 1B][payload][crc32 4B of payload]
//	payload:
//	  walGen uvarint        -- replay WAL generations >= walGen
//	  nextFileID uvarint
//	  nextSeq uvarint
//	  seriesCount uvarint
//	  per series (sorted by name):
//	    nameLen uvarint, name bytes
//	    blockCount uvarint
//	    per block (sorted by (T0, Seq); timestamps delta-encoded
//	    against the previous block of the same series):
//	      fileID uvarint, offset uvarint, length uvarint,
//	      seq uvarint, count uvarint,
//	      t0 zigzag (first block: absolute; else t0-prevT1),
//	      t1 zigzag (t1-t0)
type indexSnapshot struct {
	WALGen     uint64 // replay WAL generations >= WALGen
	NextFileID uint64 // next data file generation
	NextSeq    uint64 // next block sequence number
	Series     map[string][]BlockMeta
}

const indexFileName = "index"
const indexTmpName = "index.tmp"

var errBadIndex = errors.New("store: corrupt index")

func emptySnapshot() *indexSnapshot {
	return &indexSnapshot{
		WALGen:     0,
		NextFileID: 1,
		NextSeq:    1,
		Series:     make(map[string][]BlockMeta),
	}
}

// loadIndex reads and validates the index snapshot from dir. It returns
// an empty snapshot (and no error) when no index exists yet.
func loadIndex(dir string) (*indexSnapshot, error) {
	data, err := os.ReadFile(filepath.Join(dir, indexFileName))
	if os.IsNotExist(err) {
		return emptySnapshot(), nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 8 || data[0] != 'I' || data[1] != 'D' || data[2] != 'X' || data[3] != 1 {
		return nil, errBadIndex
	}
	payload := data[4 : len(data)-4]
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != stored {
		return nil, errBadIndex
	}
	snap := emptySnapshot()
	r := &byteReader{buf: payload}
	snap.WALGen = r.uvarint()
	snap.NextFileID = r.uvarint()
	snap.NextSeq = r.uvarint()
	nSeries := r.uvarint()
	for i := uint64(0); i < nSeries && r.err == nil; i++ {
		name := r.str()
		nBlocks := r.uvarint()
		blocks := make([]BlockMeta, 0, nBlocks)
		var prevT1 int64
		for j := uint64(0); j < nBlocks; j++ {
			var bm BlockMeta
			bm.FileID = r.uvarint()
			bm.Offset = r.uvarint()
			bm.Length = uint32(r.uvarint())
			bm.Seq = r.uvarint()
			bm.Count = int(r.uvarint())
			d0 := r.zigzag()
			d1 := r.zigzag()
			if j == 0 {
				bm.T0 = d0
			} else {
				bm.T0 = prevT1 + d0
			}
			bm.T1 = bm.T0 + d1
			prevT1 = bm.T1
			blocks = append(blocks, bm)
		}
		snap.Series[name] = blocks
	}
	if r.err != nil {
		return nil, errBadIndex
	}
	return snap, nil
}

// persist atomically writes the snapshot into dir: encode, write tmp
// file, fsync, rename over the old index, fsync the directory.
func (s *indexSnapshot) persist(dir string) error {
	payload := s.encode()
	out := make([]byte, 0, len(payload)+8)
	out = append(out, 'I', 'D', 'X', 1)
	out = append(out, payload...)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payload))
	out = append(out, crc[:]...)

	tmp := filepath.Join(dir, indexTmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(out); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, indexFileName)); err != nil {
		return err
	}
	// fsync the directory so the rename is durable
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *indexSnapshot) encode() []byte {
	buf := make([]byte, 0, 256)
	buf = appendUvarint(buf, s.WALGen)
	buf = appendUvarint(buf, s.NextFileID)
	buf = appendUvarint(buf, s.NextSeq)
	names := make([]string, 0, len(s.Series))
	for name := range s.Series {
		names = append(names, name)
	}
	sort.Strings(names)
	buf = appendUvarint(buf, uint64(len(names)))
	for _, name := range names {
		blocks := s.Series[name]
		buf = appendUvarint(buf, uint64(len(name)))
		buf = append(buf, name...)
		buf = appendUvarint(buf, uint64(len(blocks)))
		var prevT1 int64
		for j, bm := range blocks {
			buf = appendUvarint(buf, bm.FileID)
			buf = appendUvarint(buf, bm.Offset)
			buf = appendUvarint(buf, uint64(bm.Length))
			buf = appendUvarint(buf, bm.Seq)
			buf = appendUvarint(buf, uint64(bm.Count))
			if j == 0 {
				buf = appendZigzag(buf, bm.T0)
			} else {
				buf = appendZigzag(buf, bm.T0-prevT1)
			}
			buf = appendZigzag(buf, bm.T1-bm.T0)
			prevT1 = bm.T1
		}
	}
	return buf
}

// --- small local encode/decode helpers (byte-slice based) ---

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

type byteReader struct {
	buf []byte
	off int
	err error
}

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf[r.off:])
	if n <= 0 {
		r.err = errBadIndex
		return 0
	}
	r.off += n
	return v
}

func (r *byteReader) zigzag() int64 {
	ux := r.uvarint()
	v := int64(ux >> 1)
	if ux&1 != 0 {
		v = ^v
	}
	return v
}

func (r *byteReader) str() string {
	n := r.uvarint()
	if r.err != nil {
		return ""
	}
	if r.off+int(n) > len(r.buf) {
		r.err = errBadIndex
		return ""
	}
	s := string(r.buf[r.off : r.off+int(n)])
	r.off += int(n)
	return s
}

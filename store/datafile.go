package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// dataFile is an append-only file holding compressed block records.
// Record layout: [magic 2B]['B','L'][payloadLen 4B LE][payload].
// The payload is a self-checksummed codec block, so a torn tail record
// is detected at open and never referenced by the index.
type dataFile struct {
	f      *os.File
	id     uint64
	offset uint64 // current append offset
}

var (
	// errShortRecord indicates a torn record tail (crash mid-append).
	errShortRecord = errors.New("store: short block record")
	// errBadRecord indicates a record framing mismatch.
	errBadRecord = errors.New("store: bad block record")
)

const recordHeaderSize = 6

func dataFileName(id uint64) string { return fmt.Sprintf("%016x.dat", id) }

// openDataFile opens (or creates) the data file with the given id inside
// dir, positioned for appending.
func openDataFile(dir string, id uint64) (*dataFile, error) {
	path := filepath.Join(dir, dataFileName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &dataFile{f: f, id: id, offset: uint64(off)}, nil
}

// append writes one block record and returns its offset and payload
// length. The record is not fsynced; call sync.
func (d *dataFile) append(block []byte) (offset uint64, length uint32, err error) {
	var hdr [recordHeaderSize]byte
	hdr[0], hdr[1] = 'B', 'L'
	binary.LittleEndian.PutUint32(hdr[2:], uint32(len(block)))
	offset = d.offset
	if _, err := d.f.Write(hdr[:]); err != nil {
		return 0, 0, err
	}
	if _, err := d.f.Write(block); err != nil {
		return 0, 0, err
	}
	d.offset += uint64(recordHeaderSize) + uint64(len(block))
	return offset, uint32(len(block)), nil
}

// sync flushes the file to stable storage.
func (d *dataFile) sync() error { return d.f.Sync() }

// readAt reads one block record payload at the given offset and verifies
// the record framing.
func (d *dataFile) readAt(offset uint64, length uint32) ([]byte, error) {
	buf := make([]byte, recordHeaderSize+int(length))
	if _, err := d.f.ReadAt(buf, int64(offset)); err != nil {
		return nil, errShortRecord
	}
	if buf[0] != 'B' || buf[1] != 'L' {
		return nil, errBadRecord
	}
	if binary.LittleEndian.Uint32(buf[2:]) != length {
		return nil, errBadRecord
	}
	return buf[recordHeaderSize:], nil
}

// close closes the underlying file.
func (d *dataFile) close() error { return d.f.Close() }

// removeDataFile deletes the data file with the given id.
func removeDataFile(dir string, id uint64) error {
	err := os.Remove(filepath.Join(dir, dataFileName(id)))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

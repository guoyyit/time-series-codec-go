package codec

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
)

// WAL 记录格式（每条 20 字节定长）：
//
//	[0:4]  crc32(payload)
//	[4:8]  payload 长度 = 16
//	[8:16] T (int64)
//	[16:24] V (float64)
//
// 重放时遇到校验失败或半条记录即停止并截断，保证崩溃不留下坏数据。
const (
	walFileName  = "wal.log"
	walRecSize   = 4 + 4 + 8 + 8
	walPayloadLn = 16
)

// wal 是写前日志，保证崩溃后可重放未封块的样本。
type wal struct {
	f    *os.File
	dir  string
	sync bool
}

// openWAL 打开（必要时创建）目录下的 WAL，并返回待重放的记录。
func openWAL(dir string, syncWrites bool) (*wal, []Sample, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	w := &wal{f: f, dir: dir, sync: syncWrites}
	samples, err := w.replay()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return w, samples, nil
}

// replay 读取全部有效记录，截断尾部损坏数据，并将写指针移到有效末尾。
func (w *wal) replay() ([]Sample, error) {
	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	var samples []Sample
	var good int64
	buf := make([]byte, walRecSize)
	for {
		off, _ := w.f.Seek(0, 1)
		n, err := w.f.Read(buf)
		if n < walRecSize || err != nil {
			break // 文件尾或半条记录
		}
		crc := binary.LittleEndian.Uint32(buf[0:4])
		plen := binary.LittleEndian.Uint32(buf[4:8])
		if plen != walPayloadLn || crc32.ChecksumIEEE(buf[8:24]) != crc {
			break // 损坏记录：截断
		}
		t := int64(binary.LittleEndian.Uint64(buf[8:16]))
		v := math.Float64frombits(binary.LittleEndian.Uint64(buf[16:24]))
		samples = append(samples, Sample{T: t, V: v})
		good = off + walRecSize
	}
	if err := w.f.Truncate(good); err != nil {
		return nil, err
	}
	if _, err := w.f.Seek(good, 0); err != nil {
		return nil, err
	}
	return samples, nil
}

// append 追加一条样本记录并落盘。
func (w *wal) append(s Sample) error {
	var buf [walRecSize]byte
	binary.LittleEndian.PutUint64(buf[8:16], uint64(s.T))
	binary.LittleEndian.PutUint64(buf[16:24], math.Float64bits(s.V))
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[8:24]))
	binary.LittleEndian.PutUint32(buf[4:8], walPayloadLn)
	if _, err := w.f.Write(buf[:]); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

// reset 在封块完成后截断 WAL（重建空文件）。
func (w *wal) reset() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	path := filepath.Join(w.dir, walFileName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

func (w *wal) close() error { return w.f.Close() }

package storage

import (
	"io"
	"os"
	"syscall"
)

// Reader is an abstraction for file access, allowing both standard I/O and mmap.
type Reader interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// DiskReader wraps a standard *os.File.
type DiskReader struct {
	f *os.File
}

func NewDiskReader(f *os.File) *DiskReader {
	return &DiskReader{f: f}
}

func (d *DiskReader) ReadAt(b []byte, off int64) (int, error) {
	return d.f.ReadAt(b, off)
}

func (d *DiskReader) Close() error {
	return d.f.Close()
}

func (d *DiskReader) Size() int64 {
	info, err := d.f.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// MmapReader uses memory-mapped files for zero-copy reads.
type MmapReader struct {
	f    *os.File
	data []byte
	size int64
}

func NewMmapReader(path string) (*MmapReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	size := info.Size()

	if size == 0 {
		// Empty file cannot be mmapped
		return &MmapReader{f: f, data: nil, size: 0}, nil
	}

	// PROT_READ: Read only
	// MAP_SHARED: Changes are shared (though we treat it as immutable)
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &MmapReader{f: f, data: data, size: size}, nil
}

func (m *MmapReader) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 || off >= m.size {
		return 0, io.EOF
	}
	if off+int64(len(b)) > m.size {
		// Partial read at EOF
		copy(b, m.data[off:])
		return int(m.size - off), io.EOF // Return EOF for partial read to match io.ReaderAt contract? io.ReaderAt says "n < len(b) => err"
	}
	copy(b, m.data[off:off+int64(len(b))])
	return len(b), nil
}

func (m *MmapReader) Close() error {
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			return err
		}
	}
	return m.f.Close()
}

func (m *MmapReader) Size() int64 {
	return m.size
}

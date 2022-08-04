package logfile

import (
	"os"

	"github.com/szuwgh/temsearch/pkg/engine/tem/util/fileutil"
)

type Mmap struct {
	fd     *os.File
	buf    []byte // a buffer of mmap
	bufLen int64
}

func NewMmap(fName string, fsize int64) (LogFile, error) {
	if fsize < 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := fileutil.Mmap(file, true, int(fsize))
	if err != nil {
		return nil, err
	}
	return &Mmap{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

func (m *Mmap) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || offset+length > m.bufLen {
		return 0, ErrOutOfFile
	}
	return copy(m.buf[offset:], b), nil
}

func (m *Mmap) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset > m.bufLen {
		return 0, ErrOutOfFile
	}
	if offset+int64(len(b)) >= m.bufLen {
		return 0, ErrOutOfFile
	}
	return copy(b, m.buf[offset:]), nil
}

func (m *Mmap) Sync() error {
	return fileutil.Msync(m.buf)
}

func (m *Mmap) Close() error {
	if err := fileutil.Msync(m.buf); err != nil {
		return err
	}
	if err := fileutil.Munmap(m.buf); err != nil {
		return err
	}
	return m.fd.Close()
}

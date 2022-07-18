package disk

import (
	"os"
	"sync"

	"github.com/szuwgh/temsearch/pkg/engine/tem/util/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/util/fileutil"
)

type mmapAccessor struct {
	mu sync.RWMutex

	f      *os.File
	b      []byte
	offset int
}

func newMmapAccessor(f *os.File) (*mmapAccessor, error) {
	m := &mmapAccessor{}
	var err error
	m.f = f
	if _, err := m.f.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}
	stat, err := m.f.Stat()
	if err != nil {
		return nil, err
	}
	m.b, err = fileutil.Mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *mmapAccessor) seek(offset int) {
	m.offset = offset
}

func (m *mmapAccessor) decbufAt(off int) byteutil.Decbuf {

	if len(m.b) < off {
		return byteutil.WithErrDecBuf()
	}
	return byteutil.NewDecBuf(m.b[off:])
}

func (m *mmapAccessor) ReadByte() (byte, error) {
	b := m.b[m.offset]
	m.offset++
	return b, nil
}

func (m *mmapAccessor) readBytes(length int) []byte {
	b := m.b[m.offset : m.offset+length]
	m.offset += length
	return b
}

func (m *mmapAccessor) readAt(offset, length uint64) []byte {
	return m.b[offset : offset+length]
}

func (m *mmapAccessor) Release() {
	m.close()
}

func (m *mmapAccessor) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.b == nil {
		return nil
	}
	err := fileutil.Munmap(m.b)
	if err != nil {
		return err
	}
	m.b = nil
	return m.f.Close()
}

package tem

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/pkg/errors"
)

const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4
)

type Wal interface {
	log([]byte) error
	close() error
	reset(fname string) error
}

const (
	blockSize  = 1 << 15 //1 << 15 //32K
	headerSize = 7
)

type flusher interface {
	Sync() error
}

type walReader struct {
	r       io.Reader
	buf     [blockSize]byte
	i, j, n int
}

func newWalReaderForIO(r io.Reader) *walReader {
	return &walReader{
		r: r,
	}
}

func newWalReader() *walReader {
	return &walReader{}
}

func (r *walReader) next() bool {

	if r.i+headerSize < r.n {
		return true
	}
	err := r.readBlock()
	if err != nil {
		return false
	}
	return r.next()
}

func (r *walReader) readBlock() error {

	n, err := io.ReadFull(r.r, r.buf[:])
	if err != nil && err != io.ErrUnexpectedEOF {
		return err
	}
	r.n = n
	r.i, r.j = 0, 0
	return nil
}

func (r *walReader) nextChunk() (uint8, error) {
	if r.i+headerSize > r.n {
		err := r.readBlock()
		if err != nil {
			return 0, err
		}
	}
	sum, length, chunkType := r.decodeHeader(r.buf[r.i:])
	r.j = r.i + headerSize + int(length)
	checkSum := crc32.ChecksumIEEE(r.buf[r.i+6 : r.j])
	if sum != checkSum {
		return 0, errors.New("checksum mismatch")
	}
	r.i += headerSize
	return chunkType, nil
}

func (r *walReader) decodeHeader(head []byte) (checkSum uint32, length uint16, chunkType uint8) {
	checkSum = binary.LittleEndian.Uint32(head)
	length = binary.LittleEndian.Uint16(head[4:])
	chunkType = head[6]
	return
}

//读一条日志
func (r *walReader) Read(p []byte) (int, error) {
	chunkType, err := r.nextChunk()
	if err != nil {
		return 0, err
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	if chunkType == fullChunkType || chunkType == lastChunkType {
		return n, io.EOF
	}
	return n, nil
}

func (x *walReader) reset(r io.Reader) {
	x.r = r
	x.i, x.j, x.n = 0, 0, 0
}

type walWriter struct {
	f     *os.File // io.Writer
	buf   [blockSize]byte
	i, j  int
	first bool
	//f     flusher
}

func newWalWriter(name string) (*walWriter, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &walWriter{
		f: f,
	}, nil
}

func (w *walWriter) close() error {
	err := w.f.Sync()
	if err != nil {
		return err
	}
	return w.f.Close()
}

func (w *walWriter) reset(fname string) error {
	w.i, w.j = 0, 0
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

//写入一个块
func (w *walWriter) writeBlock() error {

	// for i := w.j; i < blockSize; i++ {
	// 	w.buf[i] = 0
	// }
	_, err := w.f.Write(w.buf[w.i:])
	w.i = 0
	w.j = headerSize
	//w.buf = w.buf[:0]
	return err
}

func (w *walWriter) writeHeader(last bool) {
	if w.i+headerSize > w.j || w.j > blockSize {
		panic("bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkType
		} else {
			w.buf[w.i+6] = lastChunkType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkType
		} else {
			w.buf[w.i+6] = middleChunkType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], crc32.ChecksumIEEE(w.buf[w.i+6:w.j])) //checksum校验的范围包括chunk的类型以及随后的data数据
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

func (w *walWriter) checkBlock() error {
	w.i = w.j
	w.j += headerSize
	if w.j >= blockSize {
		//w.i = 0
		//w.j = headerSize
		//写入一个块
		err := w.writeBlock()
		if err != nil {
			return err
		}
	}

	w.first = true
	return nil
}

func (w *walWriter) log(p []byte) error {
	err := w.checkBlock()
	if err != nil {
		return err
	}
	for len(p) > 0 {
		if w.j == blockSize {
			w.writeHeader(false)
			err = w.writeBlock()
			if err != nil {
				return err
			}
			w.first = false
		}
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return w.flush()
}

func (w *walWriter) writePending() error {
	w.writeHeader(true)
	_, err := w.f.Write(w.buf[w.i:w.j])
	return err
}

func (w *walWriter) flush() error {
	err := w.writePending()
	if err != nil {
		return err
	}
	if w.f != nil {
		err = w.f.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

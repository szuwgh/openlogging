package disk

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/szuwgh/temsearch/pkg/engine/tem/cache"

	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/chunks"
	"github.com/szuwgh/temsearch/pkg/engine/tem/fileutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/util"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 10240
)

const (
	INDEX = iota
	DATA
)

const (
	magic     = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	footerLen = 48

	dirChunk   = "chunk"
	dirSeries  = "series"
	dirPosting = "posting"

	LogMaxSize = 128 * MiB //100 	KB

	defaultSegmentSize = 64 * MiB //32 * MiB //1024 * 1024

	DefaultBlockCacheCapacity = 8 * MiB
)

//磁盘数据
type TableOps struct {
	bcache *cache.Cache //词典缓存
	mcache *cache.Cache //倒排表缓存
	lcache *cache.Cache //日志数据缓存
}

func NewTableOps() *TableOps {
	t := &TableOps{}
	t.bcache = cache.NewCache(cache.NewLRU(DefaultBlockCacheCapacity))
	t.mcache = cache.NewCache(cache.NewLRU(defaultSegmentSize * 10))
	t.lcache = cache.NewCache(cache.NewLRU(LogMaxSize * 10))
	return t
}

func (d *TableOps) CreateIndexFrom(dir string) (*IndexW, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	return newIndexW(dir)
}

func CreateIndexFrom(dir string) (*IndexW, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	return newIndexW(dir)
}

func (d *TableOps) CreateLogFrom(dir string) *LogW {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil
	}
	return newLogWriter(dir)
}

func CreateLogFrom(dir string) *LogW {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil
	}

	return newLogWriter(dir)
}

func (t *TableOps) CreateIndexReader(dir string, ulid ulid.ULID, baseTime int64) *IndexReader {
	bcache := &cache.NamespaceGetter{Cache: t.bcache, NS: ulid.Time()}
	mcache := &cache.NamespaceGetter{Cache: t.mcache, NS: ulid.Time()}
	return NewIndexReader(dir, baseTime, bcache, mcache)
}

func (t *TableOps) CreateLogReader(dir string, uid ulid.ULID, offset []uint64) *LogReader {
	lcache := &cache.NamespaceGetter{Cache: t.lcache, NS: uid.Time()}
	return NewLogReader(dir, offset, lcache)
}

func (t *TableOps) RemoveIndexReader(dir string, ulid ulid.ULID) error {
	t.bcache.EvictNS(ulid.Time())
	t.mcache.EvictNS(ulid.Time())
	t.lcache.EvictNS(ulid.Time())
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrapf(err, "delete obsolete block %s", ulid)
	}
	return nil
}

func LastSequenceFile(dir string) (string, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", err
	}
	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	if i == 0 {
		i++
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i)), nil
}

func NextSequenceFile(dir string) (string, int, error) {
	return nextSequenceFile(dir)
}

//下一个序列号文件
func nextSequenceFile(dir string) (string, int, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}
	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func nextSequenceExtFile(dir, ext string) (string, int, error) {
	names, err := fileutil.ReadDirFromExt(dir, ext)
	if err != nil {
		return "", 0, err
	}
	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(strings.TrimSuffix(n, ext), 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func SequenceFiles(dir string) ([]string, error) {
	return sequenceFiles(dir)
}

func sequenceFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil {
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name()))
	}
	return res, nil
}

//块
type fileWriter interface {
	//blcok写入文件系统
	//writeBlock(b []byte) blockHandle
	setTagName([]byte)
	//完成域
	finishTag() error
	//byte长度
	byteLen() int
	//完成写入
	finishBlock() error
	//关闭
	close() error
	//添加k和v
	append(k, v []byte)
	//写入byte
	write(v []byte) (int, error)

	writeBytes(b []byte) (int, error)
}

type baseWrite struct {
	w         io.WriteCloser //写入 writer
	dataBlock blockWriter    //keyblock
	footer    bytes.Buffer   //尾部

	offset   uint64 //索引文件偏移
	shareBuf []byte
}

func (bw *baseWrite) write(v []byte) (int, error) {
	return bw.dataBlock.Write(v)
}

func (bw *baseWrite) writeBytes(b []byte) (int, error) {
	n, err := bw.w.Write(b)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (bw *baseWrite) writeBlock(b []byte) (bh blockHandle, err error) {
	var n int
	n, err = bw.writeBytes(b)
	if err != nil {
		return
	}
	//写入index_block
	bh = blockHandle{bw.offset, uint64(len(b))} //- blockTailLen
	bw.offset += uint64(n)
	return
}

func (bw *baseWrite) byteLen() int {
	return bw.dataBlock.bytesLen()
}

type indexBlock interface {
	appendIndex(k []byte, bh blockHandle) error
	//	append(k, v []byte) error
	finishRestarts()
	finishTail() uint32
	reset()
	Get() []byte
}

//按页存储
type keyWriter struct {
	baseWrite
	indexBlock indexBlock //indexblock
	tagsBlock  blockWriter
	//nEntries      int //总的记录数
	baseTimeStamp int64
	tagName       []byte //标签名
	maxBlockSize  int
}

func newKeyWriter(dir string, shareBuf []byte) (*keyWriter, error) {
	kw := &keyWriter{}
	f, err := os.OpenFile(filepath.Join(dir, "index"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	kw.w = f
	kw.maxBlockSize = 4 * KB //20 //4 * KB //4kb
	kw.dataBlock.restartInterval = 2
	kw.shareBuf = shareBuf
	kw.tagsBlock.restartInterval = 1
	bw := newBlockWriter(shareBuf)
	kw.indexBlock = bw
	return kw, nil
}

func (bw *keyWriter) add(k, v []byte) error {
	if err := bw.append(k, v); err != nil {
		return err
	}
	if bw.byteLen() >= bw.maxBlockSize {
		if err := bw.finishBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (bw *keyWriter) append(k, v []byte) error {
	return bw.dataBlock.append(k, v)
}

func (bw *keyWriter) setTagName(tagName []byte) {
	bw.tagName = tagName
}

func (kw *keyWriter) finishBlock() error {
	kw.dataBlock.finishRestarts()
	kw.dataBlock.finishTail()
	bh, err := kw.writeBlock(kw.dataBlock.Get())
	if err != nil {
		return err
	}
	if err = kw.writeBlockHandleToIndex(bh); err != nil {
		return err
	}
	kw.dataBlock.reset()
	return nil
}

func (kw *keyWriter) writeBlockHandleToIndex(bh blockHandle) error {
	if err := kw.indexBlock.appendIndex(kw.dataBlock.prevKey, bh); err != nil {
		return err
	}
	kw.dataBlock.prevKey = kw.dataBlock.prevKey[:0]
	return nil
}

func (kw *keyWriter) finishTag() error {
	if kw.dataBlock.nEntries > 0 {
		if err := kw.finishBlock(); err != nil {
			return err
		}
	}
	kw.indexBlock.finishRestarts()
	kw.indexBlock.finishTail()
	bh, err := kw.writeBlock(kw.indexBlock.Get())
	if err != nil {
		return err
	}
	kw.indexBlock.reset()
	n := encodeBlockHandle(kw.shareBuf[0:], bh)
	if err = kw.tagsBlock.append(kw.tagName, kw.shareBuf[:n]); err != nil {
		return err
	}
	kw.dataBlock.reset()
	return nil
}

func (kw *keyWriter) close() error {
	if err := kw.finishBlock(); err != nil {
		return err
	}
	kw.tagsBlock.finishRestarts()
	kw.tagsBlock.finishTail()
	bh, err := kw.writeBlock(kw.tagsBlock.Get())
	if err != nil {
		return err
	}
	footer := kw.shareBuf[:footerLen]
	//重置数据
	for i := range footer {
		footer[i] = 0
	}
	encodeBlockHandle(footer, bh)
	copy(footer[footerLen-len(magic):], magic)
	if _, err = kw.w.Write(footer); err != nil {
		return err
	}
	kw.offset += footerLen
	return kw.w.Close()
}

type cutWriter struct {
	dirFile *os.File
	files   []*os.File
	fbuf    *bufio.Writer
	n       uint64
	buf1    byteutil.EncBuf
	buf2    byteutil.EncBuf
	seq     uint64
}

func (c *cutWriter) close() error {
	if err := c.finalizeTail(); err != nil {
		return err
	}
	return c.dirFile.Close()
}

func (c *cutWriter) isCut(n int) error {
	newsz := c.n + uint64(n)
	if c.fbuf == nil || c.n > defaultSegmentSize || newsz > defaultSegmentSize && n <= defaultSegmentSize {
		if err := c.cut(); err != nil {
			return err
		}
	}
	return nil
}

func (c *cutWriter) cut() error {
	if err := c.finalizeTail(); err != nil {
		return err
	}
	p, _, err := nextSequenceFile(c.dirFile.Name())
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	if err = fileutil.Preallocate(f, defaultSegmentSize, true); err != nil {
		return err
	}
	if err = c.dirFile.Sync(); err != nil {
		return err
	}
	c.files = append(c.files, f)
	if c.fbuf != nil {
		c.fbuf.Reset(f)
	} else {
		c.fbuf = bufio.NewWriterSize(f, 8*1024*1024)
	}
	c.n = 0
	c.seq++
	return nil
}

func (c *cutWriter) tail() *os.File {
	if len(c.files) == 0 {
		return nil
	}
	return c.files[len(c.files)-1]
}

func (c *cutWriter) finalizeTail() error {
	tf := c.tail()
	if tf == nil {
		return nil
	}
	if err := c.fbuf.Flush(); err != nil {
		return err
	}
	if err := fileutil.Fsync(tf); err != nil {
		return err
	}
	off, err := tf.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := tf.Truncate(off); err != nil {
		return err
	}
	return tf.Close()
}

func (c *cutWriter) writeToF(b ...[]byte) error {
	for i := range b {
		_, err := c.fbuf.Write(b[i])
		if err != nil {
			return err
		}
	}
	return nil
}

//mmap打开
type chunkWriter struct {
	cutWriter
}

func newChunkWriter(dir string) (*chunkWriter, error) {

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	cw := &chunkWriter{
		cutWriter: cutWriter{
			dirFile: dirFile,
			n:       0,
		},
	}
	return cw, nil
}

func (cw *chunkWriter) newWriter(fileName string) error {
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	if cw.fbuf != nil {
		cw.fbuf.Reset(f)
	} else {
		cw.fbuf = bufio.NewWriterSize(f, 8*1024*1024)
	}
	return nil
}

// func (cw *chunkWriter) putInt(v int) int {
// 	return cw.buf2.PutUvarint(v)
// }

// func (cw *chunkWriter) putUvarint64(v uint64) int {
// 	return cw.buf2.PutUvarint64(v)
// }

// func (cw *chunkWriter) putBytes(b []byte) (int, error) {
// 	return cw.buf2.Write(b)
// }

func (cw *seriesWriter) writeChunks(b [][]byte) (uint64, error) {
	var err error
	cw.buf1.Reset()
	cw.buf2.Reset()
	var i int
	for _, v := range b {
		i += len(v)
	}
	cw.buf2.PutUvarint(i)
	if _, err = cw.buf2.Write(b...); err != nil {
		return 0, err
	}

	crc := crc32.ChecksumIEEE(cw.buf2.Get())
	cw.buf1.PutUvarint(cw.buf2.Len())
	cw.buf2.PutUint32(crc)
	length := cw.buf1.Len() + cw.buf2.Len()

	// if err = cw.isCut(length); err != nil {
	// 	return 0, err
	// }
	if err = cw.writeToF(cw.buf1.Get(), cw.buf2.Get()); err != nil {
		return 0, err
	}
	//seq := cw.seq << 32
	//pos := seq | cw.n
	pod := cw.n
	cw.n += uint64(length)
	return pod, nil

}

type singleWriter struct {
	file *os.File
	fbuf *bufio.Writer
	buf1 byteutil.EncBuf
	buf2 byteutil.EncBuf
	n    uint64
}

func (s *singleWriter) close() error {
	if err := s.finalizeTail(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *singleWriter) init() error {
	// f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0666)
	// if err != nil {
	// 	return err
	// }
	var err error
	if err = fileutil.Preallocate(s.file, defaultSegmentSize, true); err != nil {
		return err
	}
	if err = s.file.Sync(); err != nil {
		return err
	}
	if s.fbuf != nil {
		s.fbuf.Reset(s.file)
	} else {
		s.fbuf = bufio.NewWriterSize(s.file, 8*1024*1024)
	}
	return nil
}

func (s *singleWriter) finalizeTail() error {
	if err := s.fbuf.Flush(); err != nil {
		return err
	}
	if err := fileutil.Fsync(s.file); err != nil {
		return err
	}
	off, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := s.file.Truncate(off); err != nil {
		return err
	}
	return nil
}

func (s *singleWriter) writeToF(b ...[]byte) error {
	for i := range b {
		_, err := s.fbuf.Write(b[i])
		if err != nil {
			return err
		}
	}
	return nil
}

type seriesWriter struct {
	singleWriter

	seriesOffsets map[string]uint64
}

func newSeriesWriter(dir string) (*seriesWriter, error) {

	// if err := os.MkdirAll(dir, 0777); err != nil {
	// 	return nil, err
	// }
	f, err := os.OpenFile(filepath.Join(dir, "series"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	sw := &seriesWriter{
		singleWriter: singleWriter{
			file: f,
		},
		seriesOffsets: make(map[string]uint64),
	}
	sw.init()
	return sw, nil
}

func (sw *seriesWriter) addSeries(isSeries bool, lset labels.Labels, chunks ...ChunkMeta) (uint64, error) {
	sw.buf1.Reset()
	sw.buf2.Reset()
	sw.buf2.PutUvarint(len(lset))
	for _, l := range lset {
		sw.buf2.PutUvarintStr(l.Name)
		sw.buf2.PutUvarintStr(l.Value)
	}
	sw.buf2.PutUvarint(len(chunks))
	if len(chunks) > 0 {
		c := chunks[0]
		sw.buf2.PutVarint64(c.MinT)
		sw.buf2.PutVarint64(c.MaxT - c.MinT)
		sw.buf2.PutUvarint64(c.Ref)
		sw.buf2.PutUvarint64(c.LastLogNum)

		t0 := c.MaxT
		ref0 := c.Ref
		for _, c := range chunks[1:] {
			sw.buf2.PutVarint64(c.MinT - t0)
			sw.buf2.PutVarint64(c.MaxT - c.MinT)
			t0 = c.MaxT
			sw.buf2.PutUvarint64(c.Ref - ref0)
			sw.buf2.PutUvarint64(c.LastLogNum)
			ref0 = c.Ref
		}
	}
	//写入crc32校验码
	crc := crc32.ChecksumIEEE(sw.buf2.Get())
	sw.buf1.PutUvarint(sw.buf2.Len())
	sw.buf2.PutUint32(crc)
	length := sw.buf1.Len() + sw.buf2.Len()
	// if err := sw.isCut(length); err != nil {
	// 	return 0, err
	// }
	err := sw.writeToF(sw.buf1.Get(), sw.buf2.Get())
	if err != nil {
		return 0, err
	}
	//seq := sw.seq << 32
	pos := sw.n
	if isSeries {
		sw.seriesOffsets[lset.Serialize()] = pos
	}
	sw.n += uint64(length)
	return pos, nil
}

func (sw *seriesWriter) getSeries(lset labels.Labels) (uint64, bool) {
	chunkRef, exist := sw.seriesOffsets[lset.Serialize()]
	return chunkRef, exist
}

type postingWriter struct {
	cutWriter
}

func newPostingWriter(dir string) (*postingWriter, error) {

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	pw := &postingWriter{
		cutWriter: cutWriter{
			dirFile: dirFile,
			n:       0,
		},
	}
	return pw, nil
}

func (pw *seriesWriter) writePosting(refs ...[]uint64) (uint64, error) {
	pw.buf1.Reset()
	pw.buf2.Reset()
	refCount := len(refs)
	pw.buf2.PutUvarint(refCount)
	pw.buf2.PutUvarint(len(refs[0]))
	var ref0 uint64
	var ref1 uint64
	for i, r := range refs[0] {
		pw.buf2.PutUvarint64(r - ref0)
		ref0 = r
		if refCount > 1 {
			pw.buf2.PutUvarint64(refs[1][i] - ref1)
			ref1 = refs[1][i]
		}
	}

	//写入crc32校验码
	crc := crc32.ChecksumIEEE(pw.buf2.Get())
	pw.buf1.PutUvarint(pw.buf2.Len())
	pw.buf2.PutUint32(crc)

	length := pw.buf1.Len() + pw.buf2.Len()

	// if err := pw.isCut(length); err != nil {
	// 	return 0, err
	// }
	if err := pw.writeToF(pw.buf1.Get(), pw.buf2.Get()); err != nil {
		return 0, err
	}
	//seq := pw.seq << 32
	pos := pw.n
	pw.n += uint64(length)
	return pos, nil
}

type IndexWriter interface {
	SetTagName([]byte)
	AppendKey([]byte, uint64) error
	Close() error
	FinishTag() error
	AddSeries(bool, labels.Labels, ...ChunkMeta) (uint64, error)
	WritePostings(ref ...[]uint64) (uint64, error)
	WriteChunks([][]byte) (uint64, error)
	GetSeries(labels.Labels) (uint64, bool)
	SetBaseTimeStamp(int64)
}

//写文件
type IndexW struct {
	kw *keyWriter

	//postingw *postingWriter
	seriesw *seriesWriter
	//chunkw   *chunkWriter

	valueOffset uint64
	//lastValueOffset uint64

	shareBuf [48]byte
	//	nEntries      int //总的记录数
	//fileName      string
	//	seriesOffsets map[uint64][2]uint64 // offsets of series
	dir string
}

//初始化
func newIndexW(dir string) (*IndexW, error) {
	iw := &IndexW{}

	iw.dir = dir
	var err error
	iw.kw, err = newKeyWriter(dir, iw.shareBuf[:])
	if err != nil {
		return iw, err
	}

	// iw.chunkw, err = newChunkWriter(filepath.Join(dir, dirChunk))
	// if err != nil {
	// 	return iw, err
	// }
	iw.seriesw, err = newSeriesWriter(dir)
	if err != nil {
		return iw, err
	}
	// iw.postingw, err = newPostingWriter(filepath.Join(dir, dirPosting))
	// if err != nil {
	// 	return iw, err
	// }
	return iw, nil
}

func (tw *IndexW) GetValueOffset() uint64 {
	return tw.valueOffset
}

func (tw *IndexW) SetTagName(tagName []byte) {
	tw.kw.setTagName(tagName)
}

func (tw *IndexW) AppendKey(key []byte, ref uint64) error {
	n := binary.PutUvarint(tw.shareBuf[0:], ref)
	return tw.kw.add(key, tw.shareBuf[:n])
}

func (tw *IndexW) WriteChunks(b [][]byte) (uint64, error) {
	return tw.seriesw.writeChunks(b)
}

func (tw *IndexW) GetSeries(lset labels.Labels) (uint64, bool) {
	return tw.seriesw.getSeries(lset)
}

func (tw *IndexW) AddSeries(isSeries bool, lset labels.Labels, chunks ...ChunkMeta) (uint64, error) {
	return tw.seriesw.addSeries(isSeries, lset, chunks...)
}

func (tw *IndexW) WritePostings(ref ...[]uint64) (uint64, error) {
	return tw.seriesw.writePosting(ref...)
}

func (tw *IndexW) SetBaseTimeStamp(timeStamp int64) {
	tw.kw.baseTimeStamp = timeStamp
}

func (tw *IndexW) FinishTag() error {
	return tw.kw.finishTag()
}

type writeClose interface {
	close() error
}

func CloseChain(fn ...writeClose) error {
	for i := range fn {
		if fn[i] == nil {
			continue
		}
		err := fn[i].close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (tw *IndexW) Close() error {
	return CloseChain(tw.kw, tw.seriesw)
}

//chunk writer
type LogW struct {
	cutWriter
	//	maxBlockSize int
	logId    uint64
	index    []uint64
	shareBuf [48]byte
	//	dir          string
	indexLength uint64
}

func newLogWriter(dir string) *LogW {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil
	}
	lw := &LogW{
		cutWriter: cutWriter{
			dirFile: dirFile,
			n:       0,
		},
	}
	lw.cut()
	return lw
}

func (w *LogW) WriteIndex() error {
	var err error
	for _, v := range w.index {
		binary.LittleEndian.PutUint64(w.shareBuf[0:], v)
		if err = w.writeToF(w.shareBuf[:8]); err != nil {
			return err
		}
	}
	w.indexLength = uint64(len(w.index)) * 8
	w.index = w.index[:0]
	return nil
}

func (w *LogW) WriteBytes(b []byte) error {
	return w.writeToF(b)
}

func (w *LogW) FinishLog(length uint64) (uint64, error) {
	w.logId++
	w.index = append(w.index, w.n)
	w.n += length
	var err error
	if w.n >= LogMaxSize {
		if err = w.WriteIndex(); err != nil {
			return 0, err
		}
		if err = w.writeFoot(); err != nil {
			return 0, err
		}
		if err = w.cut(); err != nil {
			return 0, err
		}
		return w.logId, nil
	}
	return 0, nil
}

func (w *LogW) writeFoot() error {
	footer := w.shareBuf[:footerLen]
	//重置数据
	for i := range footer {
		footer[i] = 0
	}
	encodeBlockHandle(footer, blockHandle{w.n, w.indexLength})
	copy(footer[footerLen-len(magic):], magic)
	return w.writeToF(footer)
}

func (w *LogW) Close() (uint64, error) {
	var err error
	if err = w.WriteIndex(); err != nil {
		return 0, err
	}
	if err = w.writeFoot(); err != nil {
		return 0, err
	}
	return w.logId, w.close()
}

type logFreqWriter struct {
	freqBuf       byteutil.EncBuf
	posBuf        byteutil.EncBuf
	skipBuf       []byteutil.EncBuf
	lastLogID     uint64
	lastTimeStamp int64
	baseTimeStamp int64
	logNum        int
	skipListLevel int
	skipInterval  int
}

func newLogFreqWriter(level int) *logFreqWriter {
	return &logFreqWriter{
		skipBuf: make([]byteutil.EncBuf, level),
	}
}

func (f *logFreqWriter) addLogID(timestamp int64, logID uint64, pos []int) error {
	util.Assert(logID > f.lastLogID, "current logid small than last logid")
	if len(pos) == 1 {
		f.freqBuf.PutUvarint64((logID-f.lastLogID)<<1 | 1)
	} else {
		f.freqBuf.PutUvarint64((logID - f.lastLogID) << 1)
		f.freqBuf.PutUvarint(len(pos))
	}
	f.logNum++
	if f.logNum%f.skipInterval == 0 {
		f.addskip(logID)
	}
	lastp := 0
	for _, p := range pos {
		f.posBuf.PutUvarint(p - lastp)
		lastp = p
	}
	f.lastLogID = logID
	f.lastTimeStamp = timestamp
	return nil
}

func (f *logFreqWriter) addskip(logID uint64) error {
	var numLevels int
	for numLevels = 0; (f.logNum%f.skipInterval == 0) && numLevels < f.skipListLevel; f.logNum /= f.skipInterval {
		numLevels++
	}
	var childPointer int
	for level := 0; level < numLevels; level++ {
		//写入跳表数据 最后一层
		f.skipBuf[level].PutUvarint64(f.lastLogID)
		f.skipBuf[level].PutUvarint(int(f.lastTimeStamp - f.baseTimeStamp))
		f.skipBuf[level].PutUvarint(f.freqBuf.Len())
		f.skipBuf[level].PutUvarint(f.posBuf.Len())
		if level > 0 {
			f.skipBuf[level].PutUvarint(childPointer)
		}
		childPointer = f.skipBuf[level].Len() //p.skipLen[level]
	}
	return nil
}

func (f *logFreqWriter) Encode() (chunks.SnapBlock, chunks.SnapBlock, []chunks.SnapBlock) {
	logFreqr := &snapByte{data: f.freqBuf.Get(), limit: uint64(f.freqBuf.Len())}
	skipr := make([]chunks.SnapBlock, f.skipListLevel)
	for i := 0; i < f.skipListLevel; i++ {
		skipr[i] = &snapByte{data: f.skipBuf[i].Get(), limit: uint64(f.skipBuf[i].Len())}
	}
	posr := &snapByte{data: f.posBuf.Get(), limit: uint64(f.posBuf.Len())}
	return logFreqr, posr, skipr
}

func (f *logFreqWriter) Bytes() [][]byte {
	return nil
}

package disk

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/cache.go"

	"github.com/sophon-lab/temsearch/pkg/engine/tem/byteutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/fileutil"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/labels"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

const (
	INDEX = iota
	DATA
)

const (
	magic     = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"
	footerLen = 48

	ExtIndex = "index"
	ExtData  = "data"
	ExtLog   = ".logs"

	dirChunk   = "chunk"
	dirSeries  = "series"
	dirPosting = "posting"

	valueMaxSize = 100 * KiB //100 	KB

	defaultSegmentSize = 1 * MiB //32 * MiB //1024 * 1024

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
	t.lcache = cache.NewCache(cache.NewLRU(valueMaxSize * 10))
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

//按页存储
type keyWriter struct {
	baseWrite
	indexBlock    blockWriter //indexblock
	tagsBlock     blockWriter
	nEntries      int //总的记录数
	baseTimeStamp int64
	tagName       []byte //标签名
}

func newKeyWriter(dir string) (*keyWriter, error) {
	kw := &keyWriter{}
	f, err := os.OpenFile(filepath.Join(dir, "index"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	kw.w = f
	kw.dataBlock.restartInterval = 2
	kw.indexBlock.restartInterval = 1
	kw.tagsBlock.restartInterval = 1
	return kw, nil
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
	n := encodeBlockHandle(kw.shareBuf[0:], bh)
	if err := kw.indexBlock.append(kw.dataBlock.prevKey, kw.shareBuf[:n]); err != nil {
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
	// As the file was pre-allocated, we truncate any superfluous zero bytes.
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

func (cw *chunkWriter) putInt(v int) int {
	return cw.buf2.PutUvarint(v)
}

func (cw *chunkWriter) putUvarint64(v uint64) int {
	return cw.buf2.PutUvarint64(v)
}

func (cw *chunkWriter) putBytes(b []byte) (int, error) {
	return cw.buf2.Write(b)
}

func (cw *chunkWriter) writeChunks(b [][]byte) (uint64, error) {
	var err error
	cw.buf2.Reset()
	var i int
	for _, v := range b {
		i += len(v)
	}
	cw.buf2.PutUvarint(i)
	if err = cw.isCut(cw.buf2.Len() + i); err != nil {
		return 0, err
	}
	if err = cw.writeToF(cw.buf2.Get()); err != nil {
		return 0, err
	}
	for _, v := range b {
		if err = cw.writeToF(v); err != nil {
			return 0, err
		}
	}
	seq := cw.seq << 32
	pos := seq | cw.n
	cw.n += uint64(cw.buf2.Len() + i)
	return pos, nil
}

type seriesWriter struct {
	cutWriter
	seriesOffsets map[string]uint64
}

func newSeriesWriter(dir string) (*seriesWriter, error) {

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	sw := &seriesWriter{
		cutWriter: cutWriter{
			dirFile: dirFile,
			n:       0,
		},
		seriesOffsets: make(map[string]uint64),
	}
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

		t0 := c.MaxT
		ref0 := c.Ref
		for _, c := range chunks[1:] {
			sw.buf2.PutVarint64(c.MinT - t0)
			sw.buf2.PutVarint64(c.MaxT - c.MinT)
			t0 = c.MaxT
			sw.buf2.PutUvarint64(c.Ref - ref0)
			ref0 = c.Ref
		}
	}
	sw.buf1.PutUvarint(sw.buf2.Len())
	if err := sw.isCut(sw.buf1.Len() + sw.buf2.Len()); err != nil {
		return 0, err
	}
	err := sw.writeToF(sw.buf2.Get())
	if err != nil {
		return 0, err
	}
	seq := sw.seq << 32
	pos := seq | sw.n
	if isSeries {
		sw.seriesOffsets[lset.Serialize()] = pos
	}
	sw.n += uint64(sw.buf2.Len())
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

func (pw *postingWriter) writePosting(refs ...[]uint64) (uint64, error) {

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
	if err := pw.isCut(pw.buf2.Len()); err != nil {
		return 0, err
	}
	if err := pw.writeToF(pw.buf2.Get()); err != nil {
		return 0, err
	}
	seq := pw.seq << 32
	pos := seq | pw.n
	pw.n += uint64(pw.buf2.Len())
	return pos, nil
}

type IndexWriter interface {
	SetTagName([]byte)
	PutInt(int) int
	PutUInt64(uint64) int
	PutBytes([]byte) (int, error)
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

	postingw *postingWriter
	seriesw  *seriesWriter
	chunkw   *chunkWriter

	maxBlockSize    int
	valueOffset     uint64
	lastValueOffset uint64

	shareBuf      [48]byte
	nEntries      int //总的记录数
	fileName      string
	seriesOffsets map[uint64][2]uint64 // offsets of series
	dir           string
}

//初始化
func newIndexW(dir string) (*IndexW, error) {
	iw := &IndexW{}
	iw.maxBlockSize = 4 * KB //4kb
	iw.dir = dir
	kw, err := newKeyWriter(dir)
	if err != nil {
		return iw, err
	}
	kw.shareBuf = iw.shareBuf[0:]
	iw.kw = kw
	iw.chunkw, err = newChunkWriter(filepath.Join(dir, dirChunk))
	if err != nil {
		return iw, err
	}
	iw.seriesw, err = newSeriesWriter(filepath.Join(dir, dirSeries))
	if err != nil {
		return iw, err
	}
	iw.postingw, err = newPostingWriter(filepath.Join(dir, dirPosting))
	if err != nil {
		return iw, err
	}
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
	if err := tw.kw.append(key, tw.shareBuf[:n]); err != nil {
		return err
	}
	if tw.kw.byteLen() >= tw.maxBlockSize {
		if err := tw.kw.finishBlock(); err != nil {
			return err
		}
	}
	return nil
}

type writeChunkFunc func() (uint64, error)

func (tw *IndexW) WriteChunks(b [][]byte) (uint64, error) {
	return tw.chunkw.writeChunks(b)
}

func (tw *IndexW) PutInt(v int) int {
	return tw.chunkw.putInt(v)
}

func (tw *IndexW) PutUInt64(v uint64) int {
	return tw.chunkw.putUvarint64(v)
}

func (tw *IndexW) PutBytes(value []byte) (int, error) {
	return tw.chunkw.putBytes(value)
}

func (tw *IndexW) GetSeries(lset labels.Labels) (uint64, bool) {
	return tw.seriesw.getSeries(lset)
}

func (tw *IndexW) AddSeries(isSeries bool, lset labels.Labels, chunks ...ChunkMeta) (uint64, error) {
	return tw.seriesw.addSeries(isSeries, lset, chunks...)
}

func (tw *IndexW) WritePostings(ref ...[]uint64) (uint64, error) {
	return tw.postingw.writePosting(ref...)
}

func (tw *IndexW) SetBaseTimeStamp(timeStamp int64) {
	tw.kw.baseTimeStamp = timeStamp
}

// func (tw *IndexW) FinishAppend(length int) (uint64, error) {
// 	//完成keyblock写入
// 	if tw.kw.byteLen() >= tw.maxBlockSize {
// 		tw.kw.finishBlock()
// 	}
// 	return 0, nil
// }

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
	return CloseChain(tw.kw, tw.chunkw, tw.seriesw, tw.postingw)
}

type logWriter struct {
	baseWrite
	fileOffset  uint64
	indexBlock  blockWriter //indexblock
	indexLength uint64
}

func newlogWriter(fileName, ext string) *logWriter {
	lw := &logWriter{}
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil
	}
	lw.w = f
	lw.dataBlock.restartInterval = 1
	lw.indexBlock.restartInterval = 1
	lw.shareBuf = make([]byte, footerLen)
	return lw
}

func (lw *logWriter) writeBytes(b []byte) (int, error) {
	n, err := lw.baseWrite.writeBytes(b)
	if err != nil {
		return 0, err
	}
	lw.fileOffset += uint64(n)
	return n, nil
}

func (lw *logWriter) finishBlock() error {
	if lw.dataBlock.bytesLen() <= 0 {
		return nil
	}
	_, err := lw.writeBlock(lw.dataBlock.Get())
	if err != nil {
		return err
	}
	//lw.indexLength += bh.length
	lw.dataBlock.reset()
	return nil
}

func (lw *logWriter) close() error {
	if err := lw.finishBlock(); err != nil {
		return err
	}
	footer := lw.shareBuf[:footerLen]
	//重置数据
	for i := range footer {
		footer[i] = 0
	}
	encodeBlockHandle(footer, blockHandle{lw.fileOffset, lw.indexLength})
	copy(footer[footerLen-len(magic):], magic)
	if _, err := lw.w.Write(footer); err != nil {
		return err
	}
	lw.offset += footerLen
	lw.dataBlock.reset()
	return lw.w.Close()
}

func (lw *logWriter) finishTag() error {
	return nil
}

//chunk writer
type LogW struct {
	cutWriter
	maxBlockSize int
	logId        uint64
	index        []uint64
	shareBuf     [48]byte
	dir          string
	indexLength  uint64
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
	if w.n >= valueMaxSize {
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

//块
type fileReader interface {
	readBlock(bh blockHandle, restart bool) (*blockReader, error)
	close() error
}

type fileSeek interface {
	Seek(int)
}

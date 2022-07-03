package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	//"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/szuwgh/temsearch/pkg/engine/tem/cache"
	"github.com/szuwgh/temsearch/pkg/engine/tem/chunks"
	"github.com/szuwgh/temsearch/pkg/engine/tem/series"
	"github.com/szuwgh/temsearch/pkg/temql"

	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/posting"

	"github.com/szuwgh/temsearch/pkg/engine/tem/global"
	mybin "github.com/szuwgh/temsearch/pkg/engine/tem/mybinary"
	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
	"github.com/szuwgh/temsearch/pkg/lib/prompb"
)

type keyReader struct {
	r           io.ReaderAt // io.ReaderAt //key 文件
	size        int64
	tagsBlock   *blockReader
	indexBlocks map[string]*blockReader
	fieldBH     blockHandle
	bcache      *cache.NamespaceGetter
}

func newKeyReader(dir string) (*keyReader, error) {
	kf, err := os.OpenFile(filepath.Join(dir, "index"), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	fStat, err := kf.Stat()
	if err != nil {
		return nil, err
	}
	kSize := fStat.Size()
	if kSize < footerLen {
		return nil, nil
	}
	r := &keyReader{r: kf, size: kSize}
	footer := r.readFooter()

	r.fieldBH, _ = decodeBlockHandle(footer)
	r.tagsBlock, err = r.readBlock(r.fieldBH, true)
	if err != nil {
		return nil, err
	}

	r.indexBlocks = make(map[string]*blockReader)
	tagsIterator := newBlockIterator(r.tagsBlock, nil)
	for tagsIterator.Next() {
		bh, _ := decodeBlockHandle(tagsIterator.Value())
		if bh.length == 0 {
			continue
		}
		indexBlock, err := r.readBlock(bh, true)
		if err != nil {
			continue
		}
		r.indexBlocks[string(tagsIterator.Key())] = indexBlock
	}

	return r, nil
}

func (br *keyReader) print(tagName string) error {
	indexBlock, err := br.getIndexBlock(tagName)
	if err != nil {
		return err
	}
	if indexBlock == nil {
		return nil
	}
	indexIter := newBlockIterator(indexBlock, nil)
	for indexIter.Next() {
		x, _ := decodeBlockHandle(indexIter.Value())
		fmt.Println(string(indexIter.Key()), x)
	}
	return nil
}

func (br *keyReader) find(tagName string, key []byte) []byte {
	indexBlock, err := br.getIndexBlock(tagName)
	if err != nil {
		return nil
	}
	if indexBlock == nil {
		return nil
	}
	indexIter := newBlockIterator(indexBlock, nil)
	//查找数据在哪个data block
	if !indexIter.seek(key) {
		return nil
	}
	dataBH, _ := decodeBlockHandle(indexIter.Value())

	dataIter := br.getDataIter(dataBH)
	if !dataIter.seekWithRestart(key) { //搜索
		dataIter.Release()
		return nil
	}
	v := dataIter.Value()
	k := dataIter.Key()
	dataIter.Release()
	if bytes.Compare(k, key) != 0 {
		return nil
	}
	return v
}

func (r *keyReader) getDataIter(bh blockHandle) *blockIterator {
	b, rel, err := r.readBlockCache(bh)
	if err != nil {
		return nil
	}
	return newBlockIterator(b, rel)
}

func (r *keyReader) readBlockCache(bh blockHandle) (*blockReader, cache.Releaser, error) {
	var (
		err error
		ch  *cache.Handle
	)
	if r.bcache != nil {
		ch = r.bcache.Get(bh.offset, func() (size int, value cache.Value) {
			var b *blockReader
			b, err = r.readBlock(bh, true)
			if err != nil {
				return 0, nil
			}
			return cap(b.data), b
		})
		if ch != nil {
			b, ok := ch.Value().(*blockReader)
			if !ok {
				ch.Release()
			}
			return b, ch, nil
		}
	}
	b, err := r.readBlock(bh, true)
	if err != nil {
		return nil, nil, err
	}
	return b, b, nil
}

func (br *keyReader) getIndexBlock(tagName string) (*blockReader, error) {
	return br.indexBlocks[tagName], nil
}

func (br *keyReader) readFooter() []byte {
	kfooterOffset := br.size - footerLen
	var footer [footerLen]byte
	if _, err := br.r.ReadAt(footer[:], kfooterOffset); err != nil && err != io.EOF {
		fmt.Println(err)
		return nil
	}
	return footer[0:]
}

//读一个块
func (tr *keyReader) readBlock(bh blockHandle, restart bool) (*blockReader, error) {
	//后面加内存池内存池复用
	// if _, err := tr.r.ReadAt(tr.shareBuf[0:], int64(bh.offset)); err != nil && err != io.EOF {
	// 	return nil, err
	// }
	data := make([]byte, int(bh.length))
	if _, err := tr.r.ReadAt(data, int64(bh.offset)); err != nil && err != io.EOF {
		return nil, err
	}
	n := int(bh.length) - blockTailLen + 1
	bh.length -= uint64(blockTailLen)
	checksum0 := binary.LittleEndian.Uint32(data[n : n+4])
	checksum1 := crc32.ChecksumIEEE(data[:bh.length])
	//校验crc
	if checksum0 != checksum1 {
		return nil, fmt.Errorf("checksum mismatch, want=%#x got=%#x", checksum0, checksum1)
	}
	data = data[:bh.length]
	restartsLen := 0
	restartsOffset := -1
	free := int(bh.length)
	if restart {
		restartsLen = int(binary.LittleEndian.Uint32(data[len(data)-4:]))
		restartsOffset = len(data) - (restartsLen+1)*4
		free = free - (restartsLen+1)*4
	}
	block := &blockReader{
		data:           data,
		free:           free,
		restartsLen:    restartsLen,
		restartsOffset: restartsOffset,
	}
	return block, nil
}

func (r *keyReader) close() error {
	if closer, ok := r.r.(io.Closer); ok {
		return closer.Close()
	}
	r.tagsBlock.Release()
	for k, v := range r.indexBlocks {
		v.Release()
		delete(r.indexBlocks, k)
	}
	r.indexBlocks = nil
	return nil
}

type chunkReader struct {
	//mmaps    []*mmapAccessor
	baseTime int64
	//	buf1     [binary.MaxVarintLen32]byte
	//	buf2     [binary.MaxVarintLen32]byte
	cutReader
}

func newchunkReader(dir string, baseTime int64, mcache *cache.NamespaceGetter) *chunkReader {
	chunkr := &chunkReader{}
	chunkr.dir = dir
	chunkr.baseTime = baseTime
	chunkr.mcache = mcache
	chunkr.ns = 3
	return chunkr
}

func (cr *seriesReader) ReadChunk(isTerm bool, ref ...uint64) chunks.ChunkEnc {
	if isTerm {
		return cr.readTermChunk(ref[0])
	}
	return cr.readLabelChunk(ref[0])
}

func (cr *seriesReader) readLabelChunk(ref uint64) *chunks.SeriesSnapShot {
	off := int(ref)
	//mmap := pr.mmaps[seq]
	mmap := cr.mmap
	// defer func() {
	// 	if rel != nil {
	// 		rel.Release()
	// 	}
	// }()
	if mmap == nil {
		return nil
	}
	debuf := mmap.decbufAt(off)

	length := debuf.uvarint()
	b := debuf.bytes(length)
	if crc32.ChecksumIEEE(b) != debuf.uint32() {
		return nil
	}
	debuf.reset(b)

	snap := chunks.NewSeriesSnapShot()
	seriesSnap := &memSeriesSnapReader{}
	seriesSnap.r = debuf
	snap.SetSnapReader(seriesSnap)
	return snap
}

func (cr *seriesReader) readTermChunk(ref uint64) *chunks.TermSnapShot {
	off := int(ref)
	//mmap := pr.mmaps[seq]
	mmap := cr.mmap
	if mmap == nil {
		return nil
	}
	debuf := mmap.decbufAt(off)

	length := debuf.uvarint()
	b := debuf.bytes(length)
	if crc32.ChecksumIEEE(b) != debuf.uint32() {
		return nil
	}
	debuf.reset(b)

	//segmentNum := debuf.uvarint64()
	snap := chunks.NewTermSnapShot() //&chunks.SnapShot{}
	snap.SetTimeStamp(cr.baseTime)
	termSnap := &memTermSnapReader{}
	termSnap.r = debuf
	snap.SetSnapReader(termSnap)
	return snap
}

type memSeriesSnapReader struct {
	r decbuf
}

func (m *memSeriesSnapReader) Encode() chunks.SnapBlock {
	m.r.uvarint()
	//segmentNum := m.r.uvarint64()
	seriesLen := m.r.uvarint64()
	seriesBytes := m.r.bytes(int(seriesLen))
	return &snapByte{data: seriesBytes, limit: seriesLen}
}

func (m *memSeriesSnapReader) Bytes() [][]byte {
	l := m.r.uvarint()
	return [][]byte{m.r.bytes(l)}
}

type memTermSnapReader struct {
	r decbuf
}

func (m *memTermSnapReader) Encode() (chunks.SnapBlock, chunks.SnapBlock, []chunks.SnapBlock) {
	m.r.uvarint()
	//	segmentNum := m.r.uvarint64()
	logFreqLen := m.r.uvarint64()
	var skipLen [global.FreqSkipListLevel]uint64
	for i := 0; i < global.FreqSkipListLevel; i++ {
		skipLen[i] = m.r.uvarint64()
	}
	posLen := m.r.uvarint64()
	logFreqr := &snapByte{data: m.r.bytes(int(logFreqLen)), limit: logFreqLen}
	var skipr [global.FreqSkipListLevel]chunks.SnapBlock
	for i := 0; i < global.FreqSkipListLevel; i++ {
		skipr[i] = &snapByte{data: m.r.bytes(int(skipLen[i])), limit: skipLen[i]}
	}
	posr := &snapByte{data: m.r.bytes(int(posLen)), limit: posLen}
	return logFreqr, posr, skipr
}

func (m *memTermSnapReader) Bytes() [][]byte {
	l := m.r.uvarint()
	return [][]byte{m.r.bytes(l)}
}

func (cr *chunkReader) release() {
	// for _, v := range cr.mmaps {
	// 	v.close()
	// }
	cr.mcache = nil
}

type cutReader struct {
	ns     uint64
	mcache *cache.NamespaceGetter
	dir    string
}

func (r *cutReader) getMmapCache(seq uint64) (*mmapAccessor, cache.Releaser) {
	ch := r.mcache.Get(r.ns<<32|seq, func() (size int, value cache.Value) {
		vf, err := os.OpenFile(filepath.Join(r.dir, fmt.Sprintf("%0.6d", seq)), os.O_RDONLY, 0644)
		if err != nil {
			return 0, nil
		}
		mmap, err := newMmapAccessor(vf)
		if err != nil {
			return 0, nil
		}
		return cap(mmap.b), mmap
	})
	if ch != nil {
		b, ok := ch.Value().(*mmapAccessor)
		if !ok {
			ch.Release()
		}
		return b, ch
	}
	return nil, nil
}

type singleReader struct {
	mmap *mmapAccessor
}

type seriesReader struct {
	//mmaps []*mmapAccessor
	singleReader
	baseTime int64

	//cutReader
}

func newSeriesReader(dir string) (*seriesReader, error) {
	seriesr := &seriesReader{}
	vf, err := os.OpenFile(filepath.Join(dir, "series"), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	mmap, err := newMmapAccessor(vf)
	if err != nil {
		return nil, err
	}
	seriesr.mmap = mmap
	return seriesr, nil
}

func (pr *seriesReader) close() error {
	return pr.mmap.close()
}

func (pr *seriesReader) getByID(ref uint64) (labels.Labels, []ChunkMeta, error) {
	//seq := ref >> 32
	off := int(ref)
	//mmap := pr.mmaps[seq]
	mmap := pr.mmap
	if mmap == nil {
		return nil, nil, errors.New("mmap is nil")
	}
	debuf := mmap.decbufAt(off)
	length := debuf.uvarint()
	b := debuf.bytes(length)
	if crc32.ChecksumIEEE(b) != debuf.uint32() {
		return nil, nil, errors.New("crc not match")
	}
	debuf.reset(b)
	k := debuf.uvarint()
	var lsets labels.Labels
	for i := 0; i < k; i++ {
		n := debuf.uvarintStr()
		v := debuf.uvarintStr()
		lsets = append(lsets, labels.Label{Name: n, Value: v})
	}
	//chunks meta data
	k = debuf.uvarint()
	for k == 0 {
		return nil, nil, nil
	}
	var chunkMeta []ChunkMeta
	t0 := debuf.varint64()
	maxt := int64(debuf.varint64()) + t0
	ref0 := debuf.uvarint64()
	segmentNum := debuf.uvarint64()

	chunkMeta = append(chunkMeta, ChunkMeta{
		Ref:        ref0,
		MinT:       t0,
		MaxT:       maxt,
		LastLogNum: segmentNum,
	})
	t0 = maxt

	for i := 1; i < k; i++ {
		mint := debuf.varint64() + t0
		maxt := debuf.varint64() + mint

		ref0 += debuf.uvarint64()
		t0 = maxt
		segmentNum := debuf.uvarint64()
		if debuf.err() != nil {
			return nil, nil, fmt.Errorf("read meta for chunk %s", debuf.err())
		}

		chunkMeta = append(chunkMeta, ChunkMeta{
			Ref:        ref0,
			MinT:       mint,
			MaxT:       maxt,
			LastLogNum: segmentNum,
		})
	}
	return lsets, chunkMeta, nil
}

func (pr *seriesReader) GetByID(ref uint64) (labels.Labels, []chunks.Chunk, error) {
	lset, chkMeta, err := pr.getByID(ref)
	chks := make([]chunks.Chunk, len(chkMeta))
	for i, v := range chkMeta {
		chks[i] = v
	}
	return lset, chks, err
}

// func (pr *seriesReader) release() {
// 	// for _, v := range pr.mmaps {
// 	// 	v.close()
// 	// }
// 	pr.mcache = nil
// }

type postingReader struct {
	cutReader
}

func newPostingReader(dir string, mcache *cache.NamespaceGetter) *postingReader {
	postingr := &postingReader{}
	postingr.dir = dir
	postingr.mcache = mcache
	postingr.ns = 1
	return postingr
}

// func (pr *seriesReader) release() {
// 	pr.mcache = nil
// }

func (pr *seriesReader) readPosting(ref uint64) ([]uint64, map[uint64]uint64) {
	off := int(ref)
	//mmap := pr.mmaps[seq]
	mmap := pr.mmap
	// defer func() {
	// 	if rel != nil {
	// 		rel.Release()
	// 	}
	// }()
	if mmap == nil {
		return nil, nil
	}
	debuf := mmap.decbufAt(off)

	length := debuf.uvarint()

	b := debuf.bytes(length)
	if crc32.ChecksumIEEE(b) != debuf.uint32() {
		return nil, nil
	}
	debuf.reset(b)

	refCount := debuf.uvarint()
	refLen := debuf.uvarint()
	seriesRef := make([]uint64, refLen)
	var termRef map[uint64]uint64
	if refCount > 1 {
		termRef = make(map[uint64]uint64, refLen)
	}
	var termRef0 uint64
	var termRef1 uint64
	for i := 0; i < refLen; i++ {
		if i == 0 {
			seriesRef[i] = debuf.uvarint64()
		} else {
			seriesRef[i] = seriesRef[i-1] + debuf.uvarint64()
		}
		if refCount > 1 {
			termRef1 = debuf.uvarint64()
			ref := termRef0 + termRef1
			termRef[seriesRef[i]] = ref
			termRef0 = ref
		}
	}
	return seriesRef, termRef
}

func (pr *seriesReader) readPosting2(ref uint64) ([]uint64, []uint64) {
	off := int(ref)
	//mmap := pr.mmaps[seq]
	mmap := pr.mmap
	if mmap == nil {
		return nil, nil
	}
	debuf := mmap.decbufAt(off)

	length := debuf.uvarint()
	b := debuf.bytes(length)
	if crc32.ChecksumIEEE(b) != debuf.uint32() {
		return nil, nil
	}
	debuf.reset(b)

	refCount := debuf.uvarint()
	refLen := debuf.uvarint()
	seriesRef := make([]uint64, refLen)
	var termRef []uint64
	if refCount > 1 {
		termRef = make([]uint64, refLen)
	}
	var termRef0 uint64
	var termRef1 uint64
	for i := 0; i < refLen; i++ {
		if i == 0 {
			seriesRef[i] = debuf.uvarint64()
		} else {
			seriesRef[i] = seriesRef[i-1] + debuf.uvarint64()
		}
		if refCount > 1 {
			termRef1 = debuf.uvarint64()
			ref := termRef0 + termRef1
			termRef[i] = ref
			termRef0 = ref
		}
	}
	return seriesRef, termRef
}

type IndexReader struct {
	mu     sync.RWMutex
	indexr *keyReader

	//	chunkr   *chunkReader
	seriesr *seriesReader
	//	postingr *postingReader
}

func (r *IndexReader) Iterator() IteratorLabel {
	iter := &tableIterator{}
	iter.labelIter = newBlockIterator(r.indexr.tagsBlock, nil)
	iter.reader = r
	//iter.chunkr = r.chunkr
	iter.seriesr = r.seriesr
	//iter.postingr = r.postingr
	return iter
}

//缺乏错误处理
func NewIndexReader(dir string, baseTime int64, bcache, mcache *cache.NamespaceGetter) *IndexReader {
	var err error
	r := &IndexReader{}
	r.seriesr, err = newSeriesReader(dir)
	r.indexr, err = newKeyReader(dir)
	if err != nil {
		return nil
	}
	r.indexr.bcache = bcache
	return r
}

func (r *IndexReader) print() error {
	for _, indexBlock := range r.indexr.indexBlocks {
		indexIterator := newBlockIterator(indexBlock, nil)
		for indexIterator.Next() {
			bh, _ := decodeBlockHandle(indexIterator.Value())
			if bh.length == 0 {
				continue
			}
			dataIterator := r.indexr.getDataIter(bh) //newBlockIterator(dataBlock)
			//dataIterator.printRestart()
			for dataIterator.Next() {
				key := dataIterator.Key()
				value := dataIterator.Value()
				ref, _ := binary.Uvarint(value)
				seriesRef, termRef := r.seriesr.readPosting(ref)
				fmt.Println("key", string(key))
				for _, v := range seriesRef {
					fmt.Print("label_series==>", v, " ")
					//log.Println(r.seriesr.readSeries(v))
					lset, chunkMeta, _ := r.seriesr.GetByID(v)
					fmt.Println(lset)
					for _, c := range chunkMeta {
						fmt.Println(c)
						chunkEnc := c.ChunkEnc(false, r.seriesr)
						//fmt.Println(chunkEnc.Bytes())
						posting := chunkEnc.Iterator(c.MinTime(), c.MaxTime(), c.SegmentNum())
						for posting.Next() {
							fmt.Print(posting.At())
							fmt.Print(" ; ")
						}
						fmt.Println(" ")
					}
				}
				for k, v := range termRef {
					fmt.Print("label_series id==>", k, " ")
					fmt.Print("term_series==>")
					lset, chunkMeta, _ := r.seriesr.GetByID(v)
					fmt.Println(lset)
					for _, c := range chunkMeta {
						fmt.Println(c)
						chunkEnc := c.ChunkEnc(true, r.seriesr)
						//fmt.Println(chunkEnc.Bytes())
						posting := chunkEnc.Iterator(c.MinTime(), c.MaxTime(), c.SegmentNum())
						for posting.Next() {
							fmt.Print(posting.At())
							fmt.Print(" ; ")
						}
						fmt.Println(" ")
					}
				}
				fmt.Println(" ")
			}
		}
	}
	return nil
}

func (r *IndexReader) ChunkReader() chunks.ChunkReader {
	return r.seriesr
}

type snapByte struct {
	data   []byte
	offset uint64
	limit  uint64
}

func (s *snapByte) Seek(offset uint64) {
	s.offset = offset
}

//ReadByte 读一个字节
func (s *snapByte) ReadByte() (byte, error) {
	if s.offset >= s.limit {
		return 0, errors.New("no content readable")
	}
	b := s.data[s.offset]
	s.offset++
	return b, nil
}

func (s *snapByte) ReadVLong() int64 {
	return mybin.Varint64(s)
}

func (s *snapByte) ReadVInt() int {
	return int(mybin.Varint64(s))
}

func (s *snapByte) ReadVUInt64() uint64 {
	return mybin.Uvarint64(s)
}

func (s *snapByte) ReadVInt64() int64 {
	return mybin.Varint64(s)
}

type termSeriesReader struct {
	refMap map[uint64]uint64
	reader *seriesReader
}

func (t termSeriesReader) GetByID(id uint64) (labels.Labels, []chunks.Chunk, error) {
	ref, ok := t.refMap[id]
	if !ok {
		return nil, nil, nil
	}
	return t.reader.GetByID(ref)
}

func (r *IndexReader) Search(lset []*prompb.LabelMatcher, expr temql.Expr) (posting.Postings, []series.Series) {
	var its []posting.Postings
	for _, v := range lset {
		value := r.find(v.Name, byteutil.Str2bytes(v.Value))
		if value == nil {
			return posting.EmptyPostings, nil
		}
		ref, _ := binary.Uvarint(value)
		list, _ := r.seriesr.readPosting(ref)
		its = append(its, posting.NewListPostings(list))
	}
	if expr == nil {
		p := posting.Intersect(its...)
		return p, []series.Series{r.seriesr}
	}
	var series []series.Series
	if len(its) > 0 {
		return posting.Intersect(queryTerm(expr, r, &series), posting.Intersect(its...)), series
	}
	return posting.Intersect(queryTerm(expr, r, &series)), series
}

func queryTerm(e temql.Expr, r *IndexReader, series *[]series.Series) posting.Postings {
	switch e.(type) {
	case *temql.TermBinaryExpr:
		expr := e.(*temql.TermBinaryExpr)
		p1 := queryTerm(expr.LHS, r, series)
		p2 := queryTerm(expr.RHS, r, series)
		switch expr.Op {
		case temql.LAND:
			return posting.Intersect(p1, p2)
		case temql.LOR:
			return posting.Merge(p1, p2)
		}
	case *temql.TermExpr:
		e := e.(*temql.TermExpr)
		value := r.find(global.MESSAGE, byteutil.Str2bytes(e.Name))
		if value == nil {
			return posting.EmptyPostings
		}
		ref, _ := binary.Uvarint(value)
		seriesRef, termMap := r.seriesr.readPosting(ref)
		*series = append(*series, &termSeriesReader{
			refMap: termMap,
			reader: r.seriesr,
		})
		return posting.NewListPostings(seriesRef)
	}
	return nil
}

func (r *IndexReader) find(tagName string, key []byte) []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.indexr.find(tagName, key)
	// indexBlock, err := r.getIndexBlock(tagName)
	// if err != nil {
	// 	return nil
	// }
	// if indexBlock == nil {
	// 	return nil
	// }
	// indexIter := newBlockIterator(indexBlock, nil)
	// //查找数据在哪个data block
	// if !indexIter.seek(key) {
	// 	return nil
	// }
	// dataBH, _ := decodeBlockHandle(indexIter.Value())

	// dataIter := r.getDataIter(dataBH)
	// if !dataIter.seekWithRestart(key) { //搜索
	// 	dataIter.Release()
	// 	return nil
	// }
	// v := dataIter.Value()
	// k := dataIter.Key()
	// dataIter.Release()
	// if bytes.Compare(k, key) != 0 {
	// 	return nil
	// }
	//return v
}

func (r *IndexReader) Close() error {
	r.indexr.close()

	//r.chunkr.release()
	//	r.postingr.release()
	r.seriesr.close()
	return nil
}

type LogReader struct {
	logOffset []uint64
	logCount  uint64
	lcache    *cache.NamespaceGetter
	dir       string
}

type logMmap struct {
	index []byte
	mmap  *mmapAccessor
}

func (r *logMmap) Release() {
	r.index = nil
	r.mmap.Release()
}

func NewLogReader(dir string, logOffset []uint64, lcache *cache.NamespaceGetter) *LogReader {

	cr := &LogReader{}
	cr.dir = dir
	cr.logOffset = logOffset
	cr.lcache = lcache
	cr.logCount = cr.logOffset[len(logOffset)-1]
	return cr
}

func (cr *LogReader) getLogMmap(logID uint64) (*logMmap, cache.Releaser, uint64) {
	var logOffset uint64
	for i := range cr.logOffset {
		if logID <= cr.logOffset[i] {
			l, ch := cr.getMmapCache(uint64(i + 1))
			return l, ch, logOffset
		}
		logOffset = cr.logOffset[i]
	}
	return nil, nil, 0
}

func (r *LogReader) getMmapCache(seq uint64) (*logMmap, cache.Releaser) {
	ch := r.lcache.Get(seq, func() (size int, value cache.Value) {
		vf, err := os.OpenFile(filepath.Join(r.dir, fmt.Sprintf("%0.6d", seq)), os.O_RDONLY, 0644)
		if err != nil {
			return 0, nil
		}
		fStat, err := vf.Stat()
		if err != nil {
			vf.Close()
			return 0, nil
		}
		s := uint64(fStat.Size())
		mmap, err := newMmapAccessor(vf)
		if err != nil {
			return 0, nil
		}
		footer := mmap.readAt(s-footerLen, footerLen)
		bh, _ := decodeBlockHandle(footer[0:])
		index := mmap.readAt(bh.offset, bh.length)
		return cap(mmap.b), &logMmap{index, mmap}
	})
	if ch != nil {
		b, ok := ch.Value().(*logMmap)
		if !ok {
			ch.Release()
		}
		return b, ch
	}
	return nil, nil
}

func (cr *LogReader) ReadLog(logID uint64) []byte {
	logMmap, ch, logOffset := cr.getLogMmap(logID)
	if logMmap == nil {
		return nil
	}
	offset := binary.LittleEndian.Uint64(logMmap.index[(logID-logOffset-1)*8:])
	debuf := logMmap.mmap.decbufAt(int(offset))
	l := debuf.uvarint()
	b := debuf.bytes(l)
	ch.Release()
	return b
}

func (cr *LogReader) Iterator() LogIterator {
	return newDiskLogIterator(cr)
}

func (cr *LogReader) Close() error {
	//cr.lcache.Cache.EvictAll()
	cr.lcache = nil
	return nil
}

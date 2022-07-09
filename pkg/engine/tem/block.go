package tem

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/szuwgh/temsearch/pkg/engine/tem/chunks"
	"github.com/szuwgh/temsearch/pkg/engine/tem/disk"
	"github.com/szuwgh/temsearch/pkg/engine/tem/fileutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/posting"
	"github.com/szuwgh/temsearch/pkg/engine/tem/series"
	"github.com/szuwgh/temsearch/pkg/lib/prompb"
	"github.com/szuwgh/temsearch/pkg/temql"
)

type IndexReader interface {
	Search(lset []*prompb.LabelMatcher, expr temql.Expr) (posting.Postings, []series.Series)
	ChunkReader() chunks.ChunkReader
	Iterator() disk.IteratorLabel
	Close() error
}

type LogReader interface {
	ReadLog(uint64) []byte
	Iterator() disk.LogIterator
	Close() error
}

type BlockControl interface {
	ReadDone()
}

type BlockReader interface {
	Index() IndexReader

	Logs() LogReader

	MinTime() int64

	MaxTime() int64

	LogNum() uint64

	LastSegNum() uint64
}

var ErrClosing = errors.New("block is closing")

type rwControl struct {
	pendingReaders sync.WaitGroup
	closing        bool
	mtx            sync.RWMutex
}

func (b *rwControl) waitRead() {
	b.mtx.Lock()
	b.closing = true
	b.mtx.Unlock()
	b.pendingReaders.Wait()
}

func (b *rwControl) startRead() error {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	if b.closing {
		return ErrClosing
	}
	b.pendingReaders.Add(1)
	return nil
}

type Block struct {
	meta       BlockMeta
	indexr     *disk.IndexReader
	logr       *disk.LogReader
	lastSegNum uint64
	rwControl
}

func (b *Block) Index() IndexReader {
	b.startRead()
	return &blockIndexReader{b.indexr, b}
}

func (b *Block) Logs() LogReader {
	b.startRead()
	return &blockLogReader{b.logr, b}
}

func (b *Block) ReadDone() {
	b.pendingReaders.Done()
}

func (b *Block) MinTime() int64 {
	return b.meta.MinTime
}

func (b *Block) MaxTime() int64 {
	return b.meta.MaxTime
}

func (b *Block) LogNum() uint64 {
	return b.meta.LogID[len(b.meta.LogID)-1]
}

func (b *Block) LastSegNum() uint64 {
	return b.lastSegNum
}

func (b *Block) Close() error {
	b.waitRead()
	var merr MultiError
	merr.Add(b.logr.Close())
	merr.Add(b.indexr.Close())

	log.Println("Block Close")
	return merr.Err()
}

type blockIndexReader struct {
	ir IndexReader
	b  BlockControl
}

func (r blockIndexReader) Search(lset []*prompb.LabelMatcher, expr temql.Expr) (posting.Postings, []series.Series) {
	return r.ir.Search(lset, expr)
}

func (r blockIndexReader) ChunkReader() chunks.ChunkReader {
	return r.ir.ChunkReader()
}

func (r blockIndexReader) Iterator() disk.IteratorLabel {
	return r.ir.Iterator()
}

func (r blockIndexReader) Close() error {
	r.b.ReadDone()
	log.Println("blockIndexReader readdone")
	return nil
}

type blockLogReader struct {
	lr LogReader
	b  BlockControl
}

func (r blockLogReader) ReadLog(v uint64) []byte {
	return r.lr.ReadLog(v)
}

func (r blockLogReader) Iterator() disk.LogIterator {
	return r.lr.Iterator()
}

func (r blockLogReader) Close() error {
	r.b.ReadDone()
	log.Println("blockLogReader readdone")
	return nil
}

type BlockMeta struct {
	// Unique identifier for the block and its contents. Changes on compaction.
	ULID ulid.ULID `json:"ulid"`

	// MinTime and MaxTime specify the time range all samples
	// in the block are in.
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`

	LogID            []uint64 `json:"log_id"`
	SkipListLevel    int      `json:"skiplistlevel"`
	SkipListInterval int      `json:"SkipListInterval"`

	Compaction BlockMetaCompaction `json:"compaction"`
}

// BlockMetaCompaction holds information about compactions a block went through.
type BlockMetaCompaction struct {
	// Maximum number of compaction cycles any source block has
	// gone through.
	Level int `json:"level"`
	// ULIDs of all source head blocks that went into the block.
	Parents []ulid.ULID `json:"parents,omitempty"`
	// Short descriptions of the direct blocks that were used to create
	// this block.
	//Parents []BlockDesc `json:"parents,omitempty"`
	//Failed  bool        `json:"failed,omitempty"`
}

type blockMeta struct {
	Version int `json:"version"`

	*BlockMeta
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}

	return dirs, nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.Parse(fi.Name())
	return err == nil
}

func writeMetaFile(dir string, meta *BlockMeta) error {
	path := filepath.Join(dir, metaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")
	var merr MultiError
	if merr.Add(enc.Encode(&blockMeta{Version: 1, BlockMeta: meta})); merr != nil {
		merr.Add(f.Close())
		return merr
	}
	if err = f.Close(); err != nil {
		return err
	}
	return renameFile(tmp, path)
}

func readMetaFile(dir string) (*BlockMeta, error) {
	b, err := ioutil.ReadFile(filepath.Join(dir, metaFilename))
	if err != nil {
		return nil, err
	}
	var m blockMeta

	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}
	return m.BlockMeta, nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fsync(pdir); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}

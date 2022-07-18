package tem

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/szuwgh/temsearch/pkg/engine/tem/util/byteutil"

	"github.com/pkg/errors"
	"github.com/szuwgh/temsearch/pkg/engine/tem/disk"
	"github.com/szuwgh/temsearch/pkg/engine/tem/util/fileutil"

	"github.com/oklog/ulid"
)

type leveledCompactor struct {
	ranges     []int64
	msgTagName string
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

func newLeveledCompactor(ranges []int64, msgTagName string) *leveledCompactor {
	compactor := &leveledCompactor{}
	compactor.ranges = ranges
	compactor.msgTagName = msgTagName
	return compactor
}

// Plan returns a list of compactable blocks in the provided directory.
func (c *leveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}

	var dms []dirMeta

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

func (c *leveledCompactor) plan(dms []dirMeta) ([]string, error) {
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	var res []string
	for _, dm := range c.selectDirs(dms) {
		res = append(res, dm.dir)
	}
	if len(res) > 0 {
		return res, nil
	}

	return nil, nil
}

func (c *leveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}

	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			continue
		}
		for _, p := range parts {
			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			if (maxt-mint >= iv || maxt <= highTime) && len(p) > 1 {
				return p
			}
		}
	}

	return nil
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		t0 = m.MinTime
		if ds[i].meta.MinTime < t0 || ds[i].meta.MaxTime >= t0+tr {
			i++
			continue
		}
		for ; i < len(ds); i++ {
			if ds[i].meta.MinTime < t0 || ds[i].meta.MaxTime >= t0+tr {
				group = append(group, ds[i])
				break
			}
			group = append(group, ds[i])
		}

		if len(group) > 0 {
			splitDirs = append(splitDirs, group)
		}
	}
	return splitDirs
}

func (c *leveledCompactor) Compact(dest string, br []BlockReader, metas []*BlockMeta) error {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)
	return c.write(dest, compactBlockMetas(uid, metas...), br...)
}

func compactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	meta := &BlockMeta{
		ULID:    uid,
		MinTime: blocks[0].MinTime,
		MaxTime: blocks[len(blocks)-1].MaxTime,
	}
	var total uint64
	for _, b := range blocks {
		if b.Compaction.Level > meta.Compaction.Level {
			meta.Compaction.Level = b.Compaction.Level
		}
		meta.Compaction.Parents = append(meta.Compaction.Parents, b.ULID)
		total += b.Total
	}
	meta.Compaction.Level++
	meta.Total = total
	return meta
}

func (c *leveledCompactor) Write(dest string, b *Head, skiplistLevel int, skiplistInterval int) (err error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)
	meta := &BlockMeta{
		ULID:             uid,
		MinTime:          b.mint,
		MaxTime:          b.MaxT,
		Total:            b.nextID,
		SkipListLevel:    skiplistLevel,
		SkipListInterval: skiplistInterval,
	}
	meta.Compaction.Level = 1
	return c.write(dest, meta, b)
}

func (c *leveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	ulid := meta.ULID.String()
	dir := filepath.Join(dest, ulid)
	tmp := dir + ".tmp"
	if err = os.RemoveAll(tmp); err != nil {
		return err
	}
	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	indexw, err := disk.CreateIndexFrom(indexDir(tmp))
	if err != nil {
		if indexw != nil {
			indexw.Close()
		}
		return err
	}
	chunkw := disk.CreateLogFrom(chunkDir(tmp))

	iters := make([]disk.IteratorLabel, len(blocks))
	Logiter := make([]disk.LogIterator, len(blocks))
	segmentNum := make([]uint64, len(blocks))
	baseTime := make([]int64, len(blocks))

	var closers = []io.Closer{}
	defer func() { closeAll(closers...) }()
	for i := range iters {
		block := blocks[i]
		indexr := block.Index()
		logr := block.Logs()
		closers = append(closers, indexr, logr)
		iters[i] = indexr.Iterator()
		Logiter[i] = logr.Iterator()

		segmentNum[i] = block.LogNum()
		baseTime[i] = block.MinTime()
	}

	err = c.composeBlock(segmentNum, baseTime, disk.NewMergeLabelIterator(iters...), disk.NewMergeLogIterator(Logiter...), meta, indexw, chunkw)
	if err != nil {
		return err
	}
	if err = writeMetaFile(tmp, meta); err != nil {
		return err
	}
	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	if err := fileutil.Fsync(df); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// close temp dir before rename block dir(for windows platform)
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil
	if err := renameFile(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}
	return nil
}

func (c *leveledCompactor) composeBlock(segmentNum []uint64, baseTime []int64,
	iter *disk.MergeLabelIterator, logIter *disk.MergeLogIterator,
	meta *BlockMeta,
	indexw disk.IndexWriter, logw disk.LogWriter) error {
	indexw.SetBaseTimeStamp(meta.MinTime)
	for iter.Next() {
		tagName := iter.Key()
		indexw.SetTagName(tagName)
		mergeIter := disk.NewMergeWriterIterator(segmentNum, baseTime, c.msgTagName, iter.Iters()...)
		for mergeIter.Next() {
			err := mergeIter.Write(byteutil.Byte2Str(tagName), indexw)
			if err != nil {
				return err
			}
		}
		indexw.FinishTag()
	}
	var err error
	err = indexw.Close()
	if err != nil {
		return err
	}
	var logID uint64
	for logIter.Next() {
		logID, err = logIter.Write(logw)
		if err != nil {
			return err
		}
		if logID != 0 {
			meta.LogID = append(meta.LogID, logID)
		}
	}
	logID, err = logw.Close()
	if err != nil {
		return err
	}
	meta.LogID = append(meta.LogID, logID)
	return nil
}

func closeAll(cs ...io.Closer) error {
	var merr MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}

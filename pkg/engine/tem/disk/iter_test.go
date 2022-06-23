package disk

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"

	"github.com/oklog/ulid"
)

func Test_IterWalk(t *testing.T) {
	// reader := NewIndexReader("E:\\goproject\\temsearch\\src\\data\\01EWCS1EKD0E9GRET66QCPVD7Z")
	// iter := reader.Iterator()
	// if iter.First() {
	// 	fmt.Println("first", string(iter.Key()), iter.Value())
	// }
	// for iter.Next() {
	// 	fmt.Println("next", iter.Key(), iter.Value())
	// }
	// for iter.NextField() {
	// 	for iter.Next() {
	// 		fmt.Println("key value", string(iter.Key()), iter.Value())
	// 	}
	// }
}

func Test_IterMerge(t *testing.T) {
	reader1 := NewIndexReader("E:\\goproject\\temsearch\\src\\data\\01F0B3H7419TB6CEZ6TY8E2N5P", 1614909121)
	iter1 := reader1.Iterator()
	reader2 := NewIndexReader("E:\\goproject\\temsearch\\src\\data\\01F0B3HPBD77RPCVAEJSMCM01J", 1614909121) //src\data\01EXGBNN7ZRE8904TWP351ZWSR
	iter2 := reader2.Iterator()

	mergeIter := NewMergeLabelIterator(iter1, iter2)
	for mergeIter.Next() {
		fmt.Println("label-->", string(mergeIter.Key()))
		iters := mergeIter.Iters()
		it := NewMergeWriterIterator(nil, nil, iters...)
		for it.Next() {

			fmt.Println("key-->", string(it.Key()))
		}
		// for i := range iters {
		// 	t := iters[i]
		// 	for t.Next() {
		// 		fmt.Println("key-->", string(t.Key()), t.Value())
		// 	}
		// }
	}
}

type BlockMeta struct {
	// Unique identifier for the block and its contents. Changes on compaction.
	ULID ulid.ULID `json:"ulid"`

	// MinTime and MaxTime specify the time range all samples
	// in the block are in.
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`

	LogID  []uint64 `json:"log_id"`
	Invert []uint64 `json:"invert"`
}

func Test_LogIter(t *testing.T) {
	dir := "E:\\goproject\\temsearch\\src\\data\\01EXNYVY84KSFR6BMNP9CN44TZ"
	b, err := ioutil.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m BlockMeta
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	logr := NewLogReader(dir, m.LogID, nil) //src\data\01EXNG1E3QYTBGGEMK9MJB443B

	iter := newDiskLogIterator(logr)
	for iter.Next() {
		t.Log(string(iter.Value()))
	}
}

func Test_CompactionMerger(t *testing.T) {
	var chks []TimeChunk
	set := labels.Label{Name: "a", Value: "b"}
	var lset labels.Labels
	lset = append(lset, set)
	var metas []ChunkMeta
	m1 := ChunkMeta{1, 2, 3}
	metas = append(metas, m1)

	t1 := TimeChunk{Lset: lset, Meta: metas}
	chks = append(chks, t1)

	merger, _ := newCompactionMerger(emptyChunkSet, emptyChunkSet)
	for merger.Next() {
		t.Log(merger.At())
	}
}

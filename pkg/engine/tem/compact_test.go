package tem

import (
	"fmt"
	"github.com/oklog/ulid"
	"github.com/szuwgh/temsearch/pkg/engine/tem/disk"
	"math/rand"
	"testing"
	"time"
)

func Test_Compact(t *testing.T) {

	fmt.Println(time.Now().UnixNano())
	opts := &Options{}
	opts.SkipListInterval = 3
	opts.SkipListLevel = 6
	opts.MsgTagName = "msg"
	e := &Engine{}
	e.tOps = disk.NewTableOps()
	e.opts = opts
	dataDir := "/opt/goproject/temsearch/src/github.com/szuwgh/temsearch/data"
	blocks, err := blockDirs(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(blocks)
	b := make([]*Block, 0, len(blocks))
	for i := range blocks {
		block, err := e.openBlock(blocks[i]) //src\data\01EXGZPPPHDTET39RRNQ7JB101
		if err != nil {
			t.Error(err)
		}
		b = append(b, block)
	}
	compactor := &leveledCompactor{}
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)
	meta := &BlockMeta{
		ULID:    uid,
		MinTime: b[0].meta.MinTime,
		MaxTime: b[len(b)-1].meta.MaxTime,
		Compaction: BlockMetaCompaction{
			Level: 2,
		},
	}
	b1 := make([]BlockReader, len(blocks))
	for i, v := range b {
		b1[i] = v
	}
	err = compactor.write(dataDir, meta, b1...)
	if err != nil {
		t.Fatal(err)
	}

}

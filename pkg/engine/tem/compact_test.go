package tem

import (
	"testing"
)

func Test_Compact(t *testing.T) {

	dataDir := "E:\\goproject\\temsearch\\src\\data"
	blocks, err := blockDirs(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(blocks)
	b := make([]BlockReader, 0, len(blocks))
	for i := range blocks {
		block, err := openBlock(blocks[i]) //src\data\01EXGZPPPHDTET39RRNQ7JB101
		if err != nil {
			t.Error(err)
		}
		b = append(b, block)
	}
	compactor := &leveledCompactor{}
	meta := newMeta(b[0].MinTime(), b[len(b)-1].MaxTime())
	err = compactor.write(dataDir, meta, b...)
	if err != nil {
		t.Fatal(err)
	}

}

// func Test_CompactPlan(t *testing.T) {
// 	compactor := newLeveledCompactor(exponentialBlockRanges(maxBlockDuration, 10, 3))

// 	plan, err := compactor.Plan("E:\\goproject\\temsearch\\src\\data")
// 	if err != nil {
// 		log.Println(err)
// 		//break
// 	}
// 	if len(plan) == 0 {
// 		//compactor.c
// 	}

// }

// func Test_exponentialBlockRanges(t *testing.T) {
// 	t0 := 10800 * (1616636751 / 10800)
// 	fmt.Println(t0)
// 	fmt.Println(exponentialBlockRanges(maxBlockDuration, 10, 3))
// }

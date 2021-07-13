package disk

import (
	"path/filepath"
	"testing"

	"github.com/oklog/ulid"
)

func Test_DiskTableReader(t *testing.T) {
	dir := "E:\\goproject\\temsearch\\src\\data\\01F6MF5X9DPCMVWAN8FMVZ7NVD"
	tOps := NewTableOps()
	ulid, _ := ulid.Parse(filepath.Base(dir))
	reader := tOps.CreateIndexReader(dir, ulid, 1622037072)
	reader.print()
}

func Test_DiskLogReader(t *testing.T) {

}

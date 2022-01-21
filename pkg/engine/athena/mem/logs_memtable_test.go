package mem

import (
	"fmt"
	"testing"

	"github.com/szuwgh/athena/pkg/engine/athena/byteutil"
)

func Test_logs(t *testing.T) {
	alloc := byteutil.NewByteBlockAllocator()
	table := NewLogsTable(byteutil.NewForwardBytePool(alloc))
	table.WriteLog([]byte("aaaaaaaaaaaaaaaaaaaaa"))
	table.WriteLog([]byte("bbbbbbbbbbbbbbbbbbbb"))
	fmt.Println(table.ReadLog(1))
	fmt.Println(table.ReadLog(2))
}

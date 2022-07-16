package disk

import (
	"testing"

	"github.com/szuwgh/temsearch/pkg/engine/tem/cache"
)

func Test_IndexReaderPrint(t *testing.T) {
	bcache := cache.NewCache(cache.NewLRU(DefaultBlockCacheCapacity))
	tcache := cache.NewCache(cache.NewLRU(defaultSegmentSize * 10))
	b := &cache.NamespaceGetter{Cache: bcache, NS: 1}
	m := &cache.NamespaceGetter{Cache: tcache, NS: 1}
	reader1 := NewIndexReader("/opt/goproject/temsearch/src/github.com/szuwgh/temsearch/pkg/engine/tem/01G82HZ99X2YCJJNHTQYE85GCN/index", 6, b, m)
	reader1.print()

}

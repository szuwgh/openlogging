package skiplist

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/szuwgh/athena/pkg/engine/athena/disk"
)

func Test_skiplist(t *testing.T) {

	sl := New(false)
	p1 := "2"
	p := unsafe.Pointer(&p1)
	sl.Insert([]byte("172.18.5.20"), p)
	sl.Insert([]byte("172.18.5.21"), p)
	sl.Insert([]byte("172.18.5.22"), p)
	sl.show()

	mergeIter := disk.NewMergeWriterIterator(nil, nil, sl.Iterator(nil, nil))
	for mergeIter.Next() {
		fmt.Println(string(mergeIter.Key()))
	}
	//sl.Insert([]byte("c"), nil)
	//fmt.Println("xxx")
	//fmt.Println(sl.Find([]byte("b")))
	//fmt.Println(sl.Find([]byte("172.18.5.21")))

}

func Test_skiplist2(t *testing.T) {
	//3-->6-->7-->9-->12---->19-->21-->25-->26
	sl := New(false)
	p1 := "2"
	p := unsafe.Pointer(&p1)
	//51 54 55 57
	sl.Insert([]byte("3"), p)
	sl.Insert([]byte("6"), p)
	sl.Insert([]byte("7"), p)
	sl.Insert([]byte("9"), p)
	sl.Insert([]byte("12"), p)
	sl.Insert([]byte("19"), p)
	sl.Insert([]byte("21"), p)
	sl.Insert([]byte("25"), p)
	sl.Insert([]byte("26"), p)
	sl.show()
	fmt.Println("xxx")
	//sl.Insert([]byte("17"), p)
	fmt.Println([]byte("12"))
	//sl.Insert([]byte("c"), nil)
	//fmt.Println("xxx")
	//fmt.Println(sl.Find([]byte("17")))
	//fmt.Println(sl.Find([]byte("a")))

}

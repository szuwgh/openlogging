package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"

	"github.com/sophon-lab/temsearch/core/engine/tem/index"
)

//后面需要优化
const (
	//KVoffset kvData开始存储数据 0
	KVoffset = iota

	KeyLen

	Valoffset
	//高度
	Height
	//Forward 向前的指针 5
	Forward

	maxLevel int     = 6    //最大层数
	p        float32 = 0.25 //概率因子

)

type skipList struct {
	nodeData []int // Node data
	keyByte  []byte
	//keyData   skipKeyStore  //[]byte      // key data
	valueData []interface{} //value data
	len       int
	level     int

	prevNode [maxLevel]int
	isTag    bool
	mu       sync.RWMutex
}

func New(isTag bool) *skipList {
	sl := &skipList{}
	//sl.termReader = newByteBlockReader(bytePool)
	sl.nodeData = append(sl.nodeData, 0)        //kvoffset
	sl.nodeData = append(sl.nodeData, 0)        //KeyLen
	sl.nodeData = append(sl.nodeData, 0)        //Valoffset
	sl.nodeData = append(sl.nodeData, maxLevel) //Height
	for i := 0; i < maxLevel; i++ {
		sl.nodeData = append(sl.nodeData, 0)
	}
	sl.isTag = isTag
	sl.level = 1
	//sl.metaLen = metaLen
	return sl
}

func (sl *skipList) IsTag() bool {
	return sl.isTag
}

func (sl *skipList) key(index int) []byte {
	kvoffset := sl.nodeData[index]
	kvLen := sl.nodeData[index+KeyLen]
	return sl.keyByte[kvoffset : kvoffset+kvLen]
}

func (sl *skipList) show() {
	for i := sl.level - 1; i >= 0; i-- {
		prev := 0
		next := sl.nodeData[prev+Forward+i]
		for next != 0 {
			fmt.Print(string(sl.key(next)), " --> ")
			next = sl.nodeData[next+Forward+i]
		}
		fmt.Println("")
	}
}

//
//两层 1/4
//三层 1/4*1/4
//n层 1/4^(n-1)
func randomLevel() int {
	level := 1
	for rand.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

/**
  x------>6------------------------------------->nil
  x------>6--------------------------->25------->nil
  x------>6------>9------------------->25------->nil
  x-->3-->6-->7-->9-->12---->19-->21-->25-->26-->nil
						  ^--17
*/
func (sl *skipList) find(key []byte, pre bool) (int, bool) {
	node := 0
	h := sl.level - 1
	for {
		next := sl.nodeData[node+Forward+h]
		cmp := 1
		if next != 0 {
			//o := sl.nodeData[next]
			cmp = bytes.Compare(sl.key(next), key)
		}
		if cmp < 0 {
			// Keep searching in this list
			node = next
		} else {
			if pre {
				sl.prevNode[h] = node
			} else if cmp == 0 {
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h--
		}
	}
}

func (sl *skipList) Find(key []byte) (interface{}, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	next, exist := sl.find(key, false)
	if exist {
		return sl.valueData[sl.nodeData[next+Valoffset]], true
	}
	return nil, false
}

//保持起始点
func (sl *skipList) Insert(key []byte, p interface{}) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	_, exist := sl.find(key, true)
	//添加内容
	if exist {
		return //sl.valueData[sl.nodeData[next+Valoffset]]
	}
	// if p == nil {
	// 	p = &RawPosting{}
	// }
	level := randomLevel()
	if level > sl.level {
		level = sl.level + 1
		sl.prevNode[level] = 0
		sl.level = level
	}
	//获取正在插入的节点
	node := len(sl.nodeData)
	sl.nodeData = append(sl.nodeData, len(sl.keyByte))
	sl.keyByte = append(sl.keyByte, key...)
	sl.nodeData = append(sl.nodeData, len(key))
	sl.nodeData = append(sl.nodeData, len(sl.valueData))
	sl.valueData = append(sl.valueData, p)

	sl.nodeData = append(sl.nodeData, level)
	for i := 0; i < level; i++ {
		m := sl.prevNode[i] + Forward + i
		sl.nodeData = append(sl.nodeData, sl.nodeData[m])
		//将最后一个小于当前的节点的向前指针置为当前的节点
		sl.nodeData[m] = node
	}
	//return p
}

func (sl *skipList) Size() int {
	return len(sl.keyByte) + len(sl.nodeData)*8
}

func (sl *skipList) Iterator() index.Iterator {
	return &skipIterator{sl: sl} //&memIterator{iter: &skipIterator{sl: sl}, chunkr: chunkr, seriesr: seriesr, isTag: sl.isTag}
}

// func (sl *skipList) Clone() postingList {
// 	return nil
// }

func (sl *skipList) Free() error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.keyByte = sl.keyByte[:0]
	sl.nodeData = sl.nodeData[:0]
	sl.valueData = sl.valueData[:0]
	return nil
}

type skipIterator struct {
	nextIndex int //迭代 next
	sl        *skipList
}

func (i *skipIterator) First() bool {
	return i.Next()
}

func (i *skipIterator) Next() bool {
	i.nextIndex = i.sl.nodeData[i.nextIndex+Forward]
	return i.nextIndex != 0
}

func (i *skipIterator) IsTag() bool {
	return i.sl.IsTag()
}

func (i *skipIterator) Value() interface{} {
	return i.sl.valueData[i.sl.nodeData[i.nextIndex+Valoffset]]
}

func (i *skipIterator) Key() []byte {
	return i.sl.key(i.nextIndex)
}

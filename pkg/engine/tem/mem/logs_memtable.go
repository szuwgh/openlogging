package mem

import (
	"encoding/binary"

	//"binary"
	//"github.com/szuwgh/temsearch/pkg/engine/tem/mybinary"
	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/disk"
)

const blockSize = 4 * 1024

//存储域
type LogsTable struct {
	bytePool byteutil.Forward
	reader   *byteutil.ForwardBytePoolReader
	index    []uint64
}

//NewFieldsTable 初始化
func NewLogsTable(bytePool byteutil.Forward) *LogsTable {
	lt := &LogsTable{}
	lt.bytePool = bytePool
	lt.reader = byteutil.NewForwardBytePoolReader(lt.bytePool)
	lt.index = make([]uint64, 0, 100)
	return lt
}

func (lt *LogsTable) ReadLog(id uint64) []byte {
	offset := lt.index[id-1]
	lt.reader.Init(offset)
	value := lt.reader.ReadString()
	return value
}

//存储field内容
func (lt *LogsTable) WriteLog(b []byte) {
	lt.index = append(lt.index, lt.bytePool.UseByteOffset())
	lt.bytePool.WriteMsg(b)
}

func (lt *LogsTable) GetByteSize(b []byte) {
	lt.index = append(lt.index, lt.bytePool.UseByteOffset())
	lt.bytePool.WriteMsg(b)
}

func (lt *LogsTable) Iterator() disk.LogIterator {
	return newForwardLogIterator(lt.reader, lt.index)
}

//回收内存
func (lt *LogsTable) Close() error {
	return nil
}

func (lt *LogsTable) ReleaseBuff(recycle, alloced *int) error {
	lt.bytePool.Release(recycle, alloced)
	lt.index = lt.index[:0]
	return nil
}

//写入文件
// func (lt *LogsTable) RotateMem(w diskWriter) {
// 	iter := lt.bytePool.Iterator()
// 	for iter.NextBuffer() {
// 		for iter.NextBytes() {
// 			w.WriteBytes(iter.Bytes())
// 		}
// 	}
// 	w.WriteIndex(lt.index)
// 	w.Close()
// }

type ForwardLogIterator struct {
	reader *byteutil.ForwardBytePoolReader
	indexs []uint64
	i      int
	share  [8]byte
}

func newForwardLogIterator(reader *byteutil.ForwardBytePoolReader, indexs []uint64) *ForwardLogIterator {
	t := &ForwardLogIterator{}
	t.reader = reader
	t.indexs = indexs
	t.i = -1
	return t
}

func (t *ForwardLogIterator) Next() bool {
	t.i++
	if t.i >= len(t.indexs) {
		return false
	}
	return true
}

func (t *ForwardLogIterator) Write(w disk.LogWriter) (uint64, error) {
	length := t.reader.InitReader(t.indexs[t.i])
	n := binary.PutUvarint(t.share[0:], length)
	w.WriteBytes(t.share[:n])
	for t.reader.Next() {
		w.WriteBytes(t.reader.Block())
	}
	return w.FinishLog(uint64(n) + length)
}

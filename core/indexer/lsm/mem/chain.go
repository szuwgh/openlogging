package mem

import (
	"sync/atomic"

	"github.com/sophon-lab/temsearch/core/indexer/lsm/byteutil"
	"github.com/sophon-lab/temsearch/core/indexer/lsm/global"
)

const (
	skipInterval = 3
)

type Context struct {
	P         *RawPosting
	Term      []byte
	LogID     uint64
	Position  int
	TimeStamp int64
}

func (c *Context) isCurLog() bool {
	return c.P.lastLogID == c.LogID
}

//IndexerFunc 索引方法
type IndexerFunc func(context *Context, memTable *MemTable)

//middleware 中间件
type middleware func(IndexerFunc) IndexerFunc

//Chain 索引链
type Chain struct {
	middlewares []middleware
}

//NewChain 一个索引链 ...
func NewChain() *Chain {
	return &Chain{}
}

//Use 添加一个中间件
func (i *Chain) Use(m middleware) {
	i.middlewares = append(i.middlewares, m)
}

//Last 最后一个方法
func (i *Chain) Last(h IndexerFunc) IndexerFunc {
	for j := range i.middlewares {
		h = i.middlewares[len(i.middlewares)-1-j](h)
	}
	return h
}

//TermMiddleware 写入词典
func TermMiddleware() middleware {
	return func(f IndexerFunc) IndexerFunc {
		return func(context *Context, memTable *MemTable) {
			p := context.P
			if p.logNum == 0 {
				//写入最小时间和最大时间
				p.byteStart = memTable.NewBlock() // 词频内存块
				p.logFreqIndex = p.byteStart
				offset := p.byteStart
				//申请minTimeStamp跳表内存块
				var skipBlockSize uint64
				for i := 0; i < global.FreqSkipListLevel; i++ {
					skipBlockSize = byteutil.SizeClass[0] * uint64(i+1)
					p.skipStartIndex[i] = offset + skipBlockSize //memTable.NewBlock() //跳表内存块
				}

				p.posIndex = offset + skipBlockSize + byteutil.SizeClass[0] //memTable.NewBlock()
				p.minT = context.TimeStamp                                  //- memTable.BaseTimeStamp
			}
			f(context, memTable)
		}
	}
}

//LogFreqMiddleware 写入词频
func LogFreqMiddleware() middleware {
	return func(f IndexerFunc) IndexerFunc {
		return func(context *Context, memTable *MemTable) {
			p := context.P
			if !p.IsCommit {
				p.logNum++
				p.maxT = context.TimeStamp //- memTable.BaseTimeStamp
				p.lastLogDelta = context.LogID - p.lastLogID
				p.lastTimeStampDelta = context.TimeStamp - p.lastTimeStamp
				p.lastLogID = context.LogID
				p.lastTimeStamp = context.TimeStamp
				p.freq++
			} else if context.isCurLog() { //当前文档
				p.freq++
			} else { //非当前文档 写入文档数
				p.logNum++
				WriteLogFreq(p, memTable)
				if p.logNum%skipInterval == 0 {
					writeSkip(context, memTable)
				}
				p.maxT = context.TimeStamp //- memTable.BaseTimeStamp
				p.lastPos = 0
				p.freq = 1

				p.lastLogDelta = context.LogID - p.lastLogID
				p.lastTimeStampDelta = context.TimeStamp - p.lastTimeStamp
				p.lastLogID = context.LogID
				p.lastTimeStamp = context.TimeStamp
			}
			f(context, memTable)
		}
	}
}

//重置
func ResetPosting(p *RawPosting, memTable *MemTable) {
	p.IsCommit = false
	p.freq = 0
	p.lastPos = 0
	p.maxT = p.lastTimeStamp //- memTable.BaseTimeStamp
}

//WriteLogFreq 写入词典信息
func WriteLogFreq(p *RawPosting, memTable *MemTable) int {
	logFreqIndex := p.logFreqIndex
	var offset uint64
	var size, length int
	//写入时间
	//offset = logFreqIndex
	offset, size = memTable.WriteVInt64(logFreqIndex, p.lastTimeStampDelta)
	if p.freq == 1 {
		offset, length = memTable.WriteVUint64(offset,
			p.lastLogDelta<<1|1)
		size += length
	} else {
		offset, length = memTable.WriteVUint64(offset, p.lastLogDelta<<1)
		size += length
		offset, length = memTable.WriteVInt(offset, p.freq)
		size += length
	}
	atomic.StoreUint64(&p.logFreqIndex, offset)
	atomic.AddUint64(&p.logFreqLen, uint64(size))
	return size
}

//writeSkip 写入跳表
func writeSkip(context *Context, memTable *MemTable) {
	p := context.P
	logNum := p.logNum //文档号
	var offset, childPointer uint64
	var size, length int

	logFreqLen := p.logFreqLen
	posLen := p.posLen
	//写入跳表
	var numLevels int
	for numLevels = 0; (logNum%skipInterval == 0) && numLevels < global.FreqSkipListLevel; logNum /= skipInterval {
		numLevels++
	}
	for level := 0; level < numLevels; level++ {
		//写入跳表数据 最后一层
		if level == 0 {
			//写入文档号 重启点
			offset, size = memTable.WriteVUint64(p.skipStartIndex[level], p.lastLogID)
			offset, length = memTable.WriteVInt(offset, int(p.lastTimeStamp-memTable.baseTimeStamp)) //memTable.BaseTimeStamp
			size += length
			offset, length = memTable.WriteVUint64(offset, logFreqLen)
			size += length
			offset, length = memTable.WriteVUint64(offset, posLen)
			size += length

			//保存相对位移
		} else {
			//写入时间 重启点
			offset, size = memTable.WriteVUint64(p.skipStartIndex[level], p.lastLogID)
			offset, length = memTable.WriteVInt(offset, int(p.lastTimeStamp-memTable.baseTimeStamp)) //memTable.BaseTimeStamp)
			size += length
			offset, length = memTable.WriteVUint64(offset, logFreqLen)
			size += length
			offset, length = memTable.WriteVUint64(offset, posLen)
			size += length
			offset, length = memTable.WriteVUint64(offset, childPointer)
			size += length
		}

		atomic.StoreUint64(&p.skipStartIndex[level], offset)
		atomic.AddUint64(&p.skipLen[level], uint64(size))
		childPointer = p.skipLen[level]

	}
}

//Position 写入位置信息
func Position(context *Context, memTable *MemTable) {
	p := context.P
	posIndex := p.posIndex
	offset, size := memTable.WriteVInt(posIndex, context.Position-context.P.lastPos)
	atomic.StoreUint64(&p.posIndex, offset)
	atomic.AddUint64(&p.posLen, uint64(size))
	p.lastPos = context.Position
}
package mem

import (
	"sync/atomic"

	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/global"

	"github.com/szuwgh/temsearch/pkg/engine/tem/util"
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

//engineFunc 索引方法
type engineFunc func(context *Context, memTable *MemTable)

//middleware 中间件
type middleware func(engineFunc) engineFunc

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
func (i *Chain) Last(h engineFunc) engineFunc {
	for j := range i.middlewares {
		h = i.middlewares[len(i.middlewares)-1-j](h)
	}
	return h
}

//TermMiddleware 写入词典
func TermMiddleware() middleware {
	return func(f engineFunc) engineFunc {
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
	return func(f engineFunc) engineFunc {
		return func(context *Context, memTable *MemTable) {
			p := context.P
			//
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
				util.Assert(context.LogID > p.lastLogID, "logid small than lastlogid")
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
func WriteLogFreq(p *RawPosting, memTable *MemTable) {
	logFreqIndex := p.logFreqIndex
	var offset uint64
	var len0, len1, len2 int
	//写入时间
	offset, len0 = memTable.WriteVInt64(logFreqIndex, p.lastTimeStampDelta)
	if p.freq == 1 {
		offset, len1 = memTable.WriteVUint64(offset,
			p.lastLogDelta<<1|1)
	} else {
		offset, len1 = memTable.WriteVUint64(offset, p.lastLogDelta<<1)
		offset, len2 = memTable.WriteVInt(offset, p.freq)
	}
	atomic.StoreUint64(&p.logFreqIndex, offset)
	atomic.AddUint64(&p.logFreqLen, uint64(len0+len1+len2))
}

//writeSkip 写入跳表
func writeSkip(context *Context, memTable *MemTable) {
	p := context.P
	logNum := p.logNum //文档号
	var offset, childPointer uint64
	var len0, len1, len2, len3, len4 int

	logFreqLen := p.logFreqLen
	posLen := p.posLen
	//写入跳表
	var numLevels int
	for numLevels = 0; (logNum%skipInterval == 0) && numLevels < memTable.skiplistLevel; logNum /= skipInterval {
		numLevels++
	}
	for level := 0; level < numLevels; level++ {
		offset, len0 = memTable.WriteVUint64(p.skipStartIndex[level], p.lastLogID)
		offset, len1 = memTable.WriteVInt(offset, int(p.lastTimeStamp-memTable.baseTimeStamp)) //memTable.BaseTimeStamp
		offset, len2 = memTable.WriteVUint64(offset, logFreqLen)
		offset, len3 = memTable.WriteVUint64(offset, posLen)
		//写入跳表数据 最后一层
		if level > 0 {
			offset, len4 = memTable.WriteVUint64(offset, childPointer)
		}
		atomic.StoreUint64(&p.skipStartIndex[level], offset)
		atomic.AddUint64(&p.skipLen[level], uint64(len0+len1+len2+len3+len4))
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

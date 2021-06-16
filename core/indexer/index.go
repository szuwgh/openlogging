package indexer

import (
	"github.com/sophon-lab/temsearch/core/concept/logmsg"
)

type IndexRow interface {
	KeySize() int
	KeyTo([]byte) (int, error)
	Key() []byte

	ValueSize() int
	ValueTo([]byte) (int, error)
	Value() []byte
}

//索引接口
type Indexer interface {
	//分析文档
	//Analyze(log *logument.Logument) *AnalysisResult
	//
	Index(logs logmsg.LogMsgArray) error
	Search()
}

// type AnalysisResult struct {
// 	LogID          string
// 	FieldInfos     FieldInfos
// 	Log            *logument.Logument
// 	FieldTermFreqs map[uint16]tokenizer.TokenFrequencies

// 	Rows []IndexRow
// }

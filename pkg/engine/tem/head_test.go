package tem

import (
	"log"
	"testing"
	"time"

	"github.com/szuwgh/temsearch/pkg/analysis"
	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/lib/logproto"
	"github.com/szuwgh/temsearch/pkg/tokenizer"
	_ "github.com/szuwgh/temsearch/pkg/tokenizer/gojieba"
)

func Test_HeadAddLog(t *testing.T) {
	tokenizer.Init()
	var logs = []logproto.Stream{
		logproto.Stream{
			Labels: `{app="nginx",instance="1.1.1.1"}`,
			Entries: []logproto.Entry{
				logproto.Entry{
					Timestamp: time.Now(),
					Line:      "Kingdee Cloud ERP is the best enterprise software",
				},
				logproto.Entry{
					Timestamp: time.Now(),
					Line:      "Kingdee Cloud·Cangqiong is an enterprise-level PaaS platform for large and super large enterprises",
				},
			},
		}, logproto.Stream{
			Labels: `{app="mysql",instance="1.2.3.4"}`,
			Entries: []logproto.Entry{
				logproto.Entry{
					Timestamp: time.Now(),
					Line:      "Kingdee is headquartered in Shenzhen, China",
				},
				logproto.Entry{
					Timestamp: time.Now(),
					Line:      "Kingdee Cloud ERP is the best enterprise software",
				},
			},
		},
	}
	h := NewHead(byteutil.NewByteBlockStackAllocator(), 20*1e3, analysis.NewAnalyzer("gojieba"), 6, 3, "~msg")
	h.open()
	for i := range logs {
		err := h.addLogs(logs[i])
		if err != nil {
			log.Fatal(err)
		}
	}
	compactor := newLeveledCompactor(nil, "~msg")
	err := compactor.Write("/opt/goproject/temsearch/src/github.com/szuwgh/temsearch/data", h, 6, 3)
	if err != nil {
		log.Fatal(err)
	}

}

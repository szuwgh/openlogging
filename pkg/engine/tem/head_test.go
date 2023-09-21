package tem

import (
	"log"
	"testing"
	"time"

	"github.com/szuwgh/hawkobserve/pkg/analysis"
	"github.com/szuwgh/hawkobserve/pkg/engine/tem/util/byteutil"
	"github.com/szuwgh/hawkobserve/pkg/lib/logproto"
	"github.com/szuwgh/hawkobserve/pkg/tokenizer"
	_ "github.com/szuwgh/hawkobserve/pkg/tokenizer/gojieba"
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
	err := compactor.Write("/opt/goproject/hawkobserve/src/github.com/szuwgh/hawkobserve/data", h, 6, 3)
	if err != nil {
		log.Fatal(err)
	}

}

package logproto

import (
	"encoding/json"
	fmt "fmt"
	"testing"
	time "time"
)

func Test_logproto(t *testing.T) {
	var p = PushRequest{
		Streams: []Stream{
			Stream{
				Labels: `{app=nginx,instance="1.1.1.1"}`,
				Entries: []Entry{
					Entry{
						Timestamp: time.Now(),
						Line:      "Kingdee Cloud ERP is the best enterprise software",
					},
					Entry{
						Timestamp: time.Now(),
						Line:      "Kingdee Cloud·Cangqiong is an enterprise-level PaaS platform for large and super large enterprises",
					},
				},
			}, Stream{
				Labels: `{app=mysql,instance="1.2.3.4"}`,
				Entries: []Entry{
					Entry{
						Timestamp: time.Now(),
						Line:      "Kingdee is headquartered in Shenzhen, China",
					},
					Entry{
						Timestamp: time.Now(),
						Line:      "Kingdee Cloud ERP is the best enterprise software",
					},
				},
			},
		},
	}
	b, _ := json.Marshal(p)
	fmt.Println(string(b))
}

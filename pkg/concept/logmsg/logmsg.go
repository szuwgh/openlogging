package logmsg

import "encoding/json"

type LogStream struct {
}

type Stream struct {
}

type LogMsgArray []*LogMsg

//文档
type LogMsg struct {
	//文档ID
	ID        string            `json:"-"`
	InterID   uint64            `json:"-"`
	Msg       string            `json:"message"` //日志信息
	Tags      map[string]string `json:"tags"`    //标签
	TimeStamp int64             `json:"timestamp"`
	Token     []string          `json:"-"`
}

func New(logID string) *LogMsg {
	return &LogMsg{
		ID: logID,
	}
}

func (l *LogMsg) Serialize() []byte {
	b, _ := json.Marshal(l)
	return b
}

func (l *LogMsg) Size() int {
	var tagSize int
	for k, v := range l.Tags {
		tagSize += len(k) + len(v)
	}
	return len(l.ID) + 8 + len(l.Msg) + tagSize + 8
}

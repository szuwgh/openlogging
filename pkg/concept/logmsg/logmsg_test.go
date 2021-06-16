package logmsg

import (
	"encoding/json"
	"fmt"
	"testing"
)

func Test_logmsg(t *testing.T) {
	logs := make(LogMsgArray, 0, 20)
	logs = append(logs, &LogMsg{
		Msg: "a",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b",
	})
	logs = append(logs, &LogMsg{
		Msg: "a",
	})
	logs = append(logs, &LogMsg{
		Msg: "dd",
	})
	logs = append(logs, &LogMsg{
		Msg: "ee",
	})
	logs = append(logs, &LogMsg{
		Msg: "f a b",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b f",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b c",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b c",
	})
	logs = append(logs, &LogMsg{
		Msg: "a b c",
	})
	data, err := json.Marshal(logs)
	fmt.Println(string(data), err)
}

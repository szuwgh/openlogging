package tem

import (
	"fmt"
	"testing"

	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
)

func Test_Pipeline(t *testing.T) {
	s := newStation(3, 128, nil)
	var id uint64 = 1
	logs1 := make(logmsg.LogMsgArray, 10)
	for i := range logs1 {
		l := &logmsg.LogMsg{}
		l.InterID = id
		id++
		l.Msg = "a,b,c,b,z"
		logs1[i] = l
	}
	fmt.Println(s.addLogs(logs1))

	logs2 := make(logmsg.LogMsgArray, 10)
	for i := range logs2 {
		l := &logmsg.LogMsg{}
		l.InterID = id
		id++
		l.Msg = "a,b,c,b,z"
		logs2[i] = l
	}
	fmt.Println(s.addLogs(logs2))

	logs3 := make(logmsg.LogMsgArray, 10)
	for i := range logs3 {
		l := &logmsg.LogMsg{}
		l.InterID = id
		id++
		l.Msg = "a,b,c,b,z"
		logs3[i] = l
	}
	fmt.Println(s.addLogs(logs3))

	logs4 := make(logmsg.LogMsgArray, 10)
	for i := range logs4 {
		l := &logmsg.LogMsg{}
		l.InterID = id
		id++
		l.Msg = "a,b,c,b,z"
		logs4[i] = l
	}
	fmt.Println(s.addLogs(logs4))

	logs5 := make(logmsg.LogMsgArray, 10)
	for i := range logs5 {
		l := &logmsg.LogMsg{}
		l.InterID = id
		id++
		l.Msg = "a,b,c,b,z"
		logs5[i] = l
	}
	fmt.Println(s.addLogs(logs5))
	fmt.Println("finish add")
	for {
		l := s.Pull()
		fmt.Println("id=", l.InterID)
	}
}

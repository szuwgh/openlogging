package tem

import (
	"github.com/pkg/errors"
	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
)

var blockErr = errors.New("log channel block")

//
type station struct {
	//data pipeline
	pipes []*pipeline
	//Looking forward to the next log, enter the index loop
	forwardID    uint64
	i            int
	logs         []*logmsg.LogMsg
	transferChan chan *logmsg.LogMsg
	j            int
}

func newStation() *station {
	s := &station{}
	s.transferChan = make(chan *logmsg.LogMsg)
	return s
}

func (s *station) transfer() {
	s.first()
	for {
		// for i := range s.pipes {
		// 	l := s.pipes[i].nextLog()
		// }
		s.logs[s.i] = s.pipes[s.i].nextLog()
		var l *logmsg.LogMsg
		for x, k := range s.logs {
			if k != nil && (k.InterID == s.forwardID) {
				s.i = x
				l = k
				s.forwardID++
			}
		}
		if l != nil {
			s.transferChan <- l
			continue
		}
		l = s.pipes[s.i].waitLog()
		if l != nil {
			s.transferChan <- l
		}
	}
}

func (s *station) addLogs(l logmsg.LogMsgArray) error {
	for i := 0; i < len(s.pipes); i++ {
		s.j = s.j % len(s.pipes)
		err := s.pipes[s.j].addLogs(l)
		if err == nil {
			return err
		}
		s.j++
	}
	return blockErr
}

func (s *station) first() {
	for i := range s.pipes {
		s.logs[i] = s.pipes[i].nextLog()
	}
}

func (s *station) pull() *logmsg.LogMsg {
	return <-s.transferChan
}

func (s *station) converge() {

}

//index pipeline
type pipeline struct {
	//log message channel
	firstChan chan logmsg.LogMsgArray

	//complete word segmentation and enter the waiting queue
	waitChan chan *logmsg.LogMsg
}

func newPipeline() *pipeline {
	p := &pipeline{}
	p.firstChan = make(chan logmsg.LogMsgArray, 128)
	p.waitChan = make(chan *logmsg.LogMsg, 512)
	go p.processTokener()
	return p
}

//add some logs
func (p *pipeline) addLogs(l logmsg.LogMsgArray) error {
	select {
	case p.firstChan <- l:
	default:
		//too many logs, pipes blocked
		return blockErr
	}
	return nil
}

//wait a log
func (p *pipeline) waitLog() *logmsg.LogMsg {
	return <-p.waitChan
}

//next a log ,no blocked
func (p *pipeline) nextLog() *logmsg.LogMsg {
	select {
	case l := <-p.waitChan:
		return l
	default:
		return nil
	}
}

//dealing with word segmentation loops
func (p *pipeline) processTokener() {
	for {
		logs := <-p.firstChan
		//tokner
		for i := range logs {
			//Enter waiting
			p.waitChan <- logs[i]
		}
	}
}

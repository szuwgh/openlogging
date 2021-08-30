package tem

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
)

var blockErr = errors.New("log channel block")

//
type station struct {
	//data pipeline
	pipes []*pipeline
	//Looking forward to the next log, enter the index loop
	forwardID uint64

	transferChan chan *logmsg.LogMsg

	j int
}

func newStation(num, bufLen int) *station {
	s := &station{}
	s.transferChan = make(chan *logmsg.LogMsg)
	s.pipes = make([]*pipeline, num)
	for i := range s.pipes {
		s.pipes[i] = newPipeline(s, bufLen, i)
	}
	s.forwardID = 1
	return s
}

//add some logs
func (s *station) addLogs(l logmsg.LogMsgArray) error {
	for i := 0; i < len(s.pipes); i++ {
		err := s.pipes[s.j].addLogs(l)
		s.j++
		if s.j == len(s.pipes) {
			s.j = 0
		}
		if err == nil {
			return err
		}
	}
	return blockErr
}

func (s *station) broadCast(ignoreNum int) {
	for i := range s.pipes {
		if i == ignoreNum {
			continue
		}
		s.pipes[i].notice()
	}
}

func (s *station) Pull() *logmsg.LogMsg {
	return <-s.transferChan
}

//index pipeline
type pipeline struct {
	num int
	//log message channel
	tokenChan chan logmsg.LogMsgArray

	//complete word segmentation and enter the waiting queue
	waitChan chan *logmsg.LogMsg

	noticeChan chan struct{}
	//
	stat *station
}

func newPipeline(stat *station, bufLen, num int) *pipeline {
	p := &pipeline{}
	p.num = num
	p.tokenChan = make(chan logmsg.LogMsgArray, bufLen)
	p.waitChan = make(chan *logmsg.LogMsg, bufLen*2)
	p.noticeChan = make(chan struct{})
	p.stat = stat
	go p.processTokener()
	go p.transfer()
	return p
}

//add some logs
func (p *pipeline) addLogs(l logmsg.LogMsgArray) error {
	select {
	case p.tokenChan <- l:
	default:
		//too many logs, pipes blocked
		return blockErr
	}
	return nil
}

//dealing with word segmentation loops
func (p *pipeline) processTokener() {
	for {
		logs := <-p.tokenChan

		for i := range logs {
			//tokener
			strings.Split(logs[i].Msg, ",")
			//Enter waiting
			p.waitChan <- logs[i]
		}
	}
}

func (p *pipeline) notice() {
	select {
	case p.noticeChan <- struct{}{}:
	default:
	}
}

func (p *pipeline) wait() {
	<-p.noticeChan
}

func (p *pipeline) transfer() {
	for {
		log := <-p.waitChan
		for {
			if log.InterID == p.stat.forwardID {
				p.stat.transferChan <- log
				p.stat.forwardID++
				p.stat.broadCast(p.num)
				break
			} else {
				p.wait()
			}
		}
	}
}

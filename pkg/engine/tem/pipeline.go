package tem

import (
	"github.com/pkg/errors"
	"github.com/sophon-lab/temsearch/pkg/concept/logmsg"
	"github.com/sophon-lab/temsearch/pkg/engine/tem/mem"
)

var blockErr = errors.New("log channel block")

type pretreatmentFunc func(*logmsg.LogMsg) mem.LogSummary

//
type station struct {
	//data pipeline
	pipes []*pipeline
	//Looking forward to the next log, enter the index loop
	forwardID uint64

	transferChan chan mem.LogSummary

	j int
}

func newStation(num, bufLen int, fn pretreatmentFunc) *station {
	s := &station{}
	s.transferChan = make(chan mem.LogSummary)
	s.pipes = make([]*pipeline, num)
	for i := range s.pipes {
		s.pipes[i] = newPipeline(s, bufLen, i, fn)
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

func (s *station) Pull() mem.LogSummary {
	return <-s.transferChan
}

//index pipeline
type pipeline struct {
	num int
	//log message channel
	tokenChan chan logmsg.LogMsgArray

	//complete word segmentation and enter the waiting queue
	waitChan chan mem.LogSummary //*logmsg.LogMsg

	noticeChan chan struct{}
	//
	stat *station
}

func newPipeline(stat *station, bufLen, num int, fn pretreatmentFunc) *pipeline {
	p := &pipeline{}
	p.num = num
	p.tokenChan = make(chan logmsg.LogMsgArray, bufLen)
	p.waitChan = make(chan mem.LogSummary, bufLen*2)
	p.noticeChan = make(chan struct{})
	p.stat = stat
	go p.processTokener(fn)
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
func (p *pipeline) processTokener(fn pretreatmentFunc) {
	for {
		logs := <-p.tokenChan
		for i := range logs {
			//log.Println("processTokener", logs[i].InterID)
			p.waitChan <- fn(logs[i])
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
		summ := <-p.waitChan
		for {
			if summ.DocID == p.stat.forwardID {
				p.stat.transferChan <- summ
				p.stat.forwardID++
				p.stat.broadCast(p.num)
				break
			} else {
				p.wait()
			}
		}
	}
}

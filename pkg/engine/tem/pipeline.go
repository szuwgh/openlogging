package tem

// import (
// 	"github.com/pkg/errors"
// 	"github.com/szuwgh/temsearch/pkg/engine/tem/mem"
// 	"github.com/szuwgh/temsearch/pkg/lib/logproto"
// )

// var blockErr = errors.New("log channel block")

// type serieserFunc func(string) *mem.MemSeries

// type tokenerFunc func(*logproto.Entry) mem.LogSummary

// //
// type station struct {
// 	//data pipeline
// 	pipes []*pipeline
// 	//Looking forward to the next log, enter the index loop
// 	forwardID uint64

// 	transferChan chan mem.LogSummary

// 	j int
// }

// func newStation(num, bufLen int, fn1 serieserFunc, fn2 tokenerFunc) *station {
// 	s := &station{}
// 	s.transferChan = make(chan mem.LogSummary)
// 	s.pipes = make([]*pipeline, num)
// 	for i := range s.pipes {
// 		s.pipes[i] = newPipeline(s, bufLen, i, fn1, fn2)
// 	}
// 	s.forwardID = 1
// 	return s
// }

// //add some logs
// func (s *station) addLogs(l logproto.Stream) error {
// 	for i := 0; i < len(s.pipes); i++ {
// 		err := s.pipes[s.j].addLogs(l)
// 		s.j++
// 		if s.j == len(s.pipes) {
// 			s.j = 0
// 		}
// 		if err == nil {
// 			return err
// 		}
// 	}
// 	return blockErr
// }

// func (s *station) broadCast(ignoreNum int) {
// 	for i := range s.pipes {
// 		if i == ignoreNum {
// 			continue
// 		}
// 		s.pipes[i].notice()
// 	}
// }

// func (s *station) Pull() mem.LogSummary {
// 	return <-s.transferChan
// }

// //index pipeline
// type pipeline struct {
// 	num int
// 	//log message channel
// 	tokenChan chan logproto.Stream

// 	//complete word segmentation and enter the waiting queue
// 	waitChan chan mem.LogSummary //*logmsg.LogMsg

// 	noticeChan chan struct{}
// 	//
// 	stat *station
// }

// func newPipeline(stat *station, bufLen, num int, fn1 serieserFunc, fn2 tokenerFunc) *pipeline {
// 	p := &pipeline{}
// 	p.num = num
// 	p.tokenChan = make(chan logproto.Stream, bufLen)
// 	p.waitChan = make(chan mem.LogSummary, bufLen*2)
// 	p.noticeChan = make(chan struct{})
// 	p.stat = stat
// 	go p.processTokener(fn1, fn2)
// 	go p.transfer()
// 	return p
// }

// //add some logs
// func (p *pipeline) addLogs(l logproto.Stream) error {
// 	select {
// 	case p.tokenChan <- l:
// 	default:
// 		//too many logs, pipes blocked
// 		return blockErr
// 	}
// 	return nil
// }

// //dealing with word segmentation loops
// func (p *pipeline) processTokener(fn1 serieserFunc, fn2 tokenerFunc) {
// 	for {
// 		logs := <-p.tokenChan
// 		//logs.Labels
// 		series := fn1(logs.Labels)
// 		for i := range logs.Entries {
// 			logSumm := fn2(&logs.Entries[i])
// 			logSumm.Series = series
// 			p.waitChan <- logSumm
// 		}
// 	}
// }

// func (p *pipeline) notice() {
// 	select {
// 	case p.noticeChan <- struct{}{}:
// 	default:
// 	}
// }

// func (p *pipeline) wait() {
// 	<-p.noticeChan
// }

// func (p *pipeline) transfer() {
// 	for {
// 		summ := <-p.waitChan
// 		for {
// 			if summ.DocID == p.stat.forwardID {
// 				p.stat.transferChan <- summ
// 				p.stat.forwardID++
// 				p.stat.broadCast(p.num)
// 				break
// 			} else {
// 				p.wait()
// 			}
// 		}
// 	}
// }

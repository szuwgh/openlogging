package chunks

import (
	glo "github.com/sophon-lab/temsearch/core/indexer/lsm/global"
)

const (
	NOMOREDOCS = 0x7fffffff

	NEXTREDOCES = -2
)

type labelPosting struct {
	logReader     SnapBlock
	lastLogID     uint64
	lastTimeStamp int64
	maxTimeStamp  int64
	minTimeStamp  int64
	segmentNum    uint64
}

func (l *labelPosting) Next() bool {
	l.nextLog()
	return !(l.lastLogID == NOMOREDOCS)
}

func (l *labelPosting) Seek(target uint64) bool {
	if l.at() != target {
		l.nextLog()
		for l.lastLogID != NOMOREDOCS && l.at() < target {
			l.nextLog()
		}
	}
	if l.lastLogID == NOMOREDOCS {
		return false
	}
	return true

}

func (l *labelPosting) At() (int64, uint64, []int) {

	return l.lastTimeStamp, l.lastLogID + l.segmentNum, nil
}

func (l *labelPosting) at() uint64 {
	return l.lastLogID + l.segmentNum
}

func (l *labelPosting) nextLog() {
	l.nextLog2()
	for l.lastTimeStamp < l.minTimeStamp && l.lastLogID != NOMOREDOCS {
		l.nextLog2()
	}
}

func (l *labelPosting) nextLog2() {
	timeStamp := l.logReader.ReadVInt64()
	l.lastTimeStamp += timeStamp
	if l.lastTimeStamp > l.maxTimeStamp {
		l.lastLogID = NOMOREDOCS
		return
	}
	logCode := l.logReader.ReadVUInt64()
	if logCode == 0 {
		l.lastLogID = NOMOREDOCS
		return
	}
	l.lastLogID += logCode
}

func (l *labelPosting) Err() error {
	return nil
}

type TermPosting struct {
	skipReader    []SnapBlock
	logFreqReader SnapBlock
	posReader     SnapBlock
	freqSeek      uint64
	prevSeek      uint64
	posSeek       uint64
	lastLogID     uint64
	lastFreq      int
	lastPos       []int
	skipLastID    [glo.FreqSkipListLevel]uint64
	lastTimeStamp int64
	timeStamp     int64
	notfirst      [glo.FreqSkipListLevel]bool

	baseTimeStamp int64
	maxTimeStamp  int64
	minTimeStamp  int64
	segmentNum    uint64
}

func (t *TermPosting) Reset() {
	for i := range t.skipReader {
		t.skipReader[i].Seek(0)
		t.skipLastID[i] = 0
		t.notfirst[i] = false
	}
	t.logFreqReader.Seek(0)
	t.posReader.Seek(0)
	t.lastTimeStamp = 0
	t.timeStamp = 0
	t.lastLogID = 0
	t.freqSeek = 0
	t.posSeek = 0
	t.prevSeek = 0
	t.lastFreq = 0
}

func (t *TermPosting) Next() bool {
	t.nextLog()
	return !(t.lastLogID == NOMOREDOCS)
}

func (t *TermPosting) Seek(target uint64) bool {
	if target == NOMOREDOCS {
		t.lastLogID = NOMOREDOCS
		return false
	}
	freqSeek, posSeek := t.freqSeek, t.posSeek
	for i := glo.FreqSkipListLevel - 1; i >= 0; i-- {
		if t.skipLastID[i] == 0 || t.notfirst[i] {
			id := t.skipReader[i].ReadVUInt64()
			if id == 0 {
				continue
			}
			t.skipLastID[i] = id
			t.timeStamp = int64(t.skipReader[i].ReadVInt()) + t.baseTimeStamp
			freqSeek = t.skipReader[i].ReadVUInt64()
			posSeek = t.skipReader[i].ReadVUInt64()
		}
		for t.skipLastID[i] != 0 {
			if t.skipLastID[i]+t.segmentNum < target {
				t.lastLogID = t.skipLastID[i]
				t.lastTimeStamp = t.timeStamp
				if i > 0 {
					t.skipReader[i-1].Seek(t.skipReader[i].ReadVUInt64())
					t.notfirst[i-1] = true
				}
				t.freqSeek = freqSeek
				t.posSeek = posSeek
				t.skipLastID[i] = t.skipReader[i].ReadVUInt64()
				t.timeStamp = t.skipReader[i].ReadVInt64() + t.baseTimeStamp
				freqSeek = t.skipReader[i].ReadVUInt64()
				posSeek = t.skipReader[i].ReadVUInt64()

			} else if t.skipLastID[i]+t.segmentNum == target {
				t.lastLogID = t.skipLastID[i]
				t.lastTimeStamp = t.timeStamp
				t.freqSeek = freqSeek
				t.posSeek = posSeek
				break
			} else {
				break
			}
		}
		t.notfirst[i] = false
	}

	if t.at() != target {
		if t.freqSeek > t.prevSeek {
			t.prevSeek = t.freqSeek
			t.logFreqReader.Seek(t.freqSeek)
			t.posReader.Seek(t.posSeek)
		}
		t.nextLog()
		for t.lastLogID != NOMOREDOCS && t.at() < target {
			t.nextLog()
		}
	} else {
		if t.lastTimeStamp > t.maxTimeStamp {
			t.lastLogID = NOMOREDOCS
		}
	}
	return true
}

func (t *TermPosting) At() (int64, uint64, []int) {
	return t.lastTimeStamp, t.lastLogID + t.segmentNum, t.lastPos
}

func (t *TermPosting) at() uint64 {
	return t.lastLogID + t.segmentNum
}

func (t *TermPosting) Err() error {
	return nil
}

func (t *TermPosting) nextLog2() {
	timeStamp := int64(t.logFreqReader.ReadVInt())
	t.lastTimeStamp += timeStamp

	if t.lastTimeStamp > t.maxTimeStamp {
		t.lastLogID = NOMOREDOCS
		return
	}
	logCode := t.logFreqReader.ReadVUInt64()
	if logCode == 0 {
		t.lastLogID = NOMOREDOCS
		return
	}
	t.lastLogID += (logCode >> 1)
	if (logCode & 1) != 0 {
		t.lastFreq = 1
	} else {
		t.lastFreq = t.logFreqReader.ReadVInt()
	}
	var pos []int
	var lastPos int
	for i := 0; i < t.lastFreq; i++ {
		delta := t.posReader.ReadVInt()
		lastPos += delta
		pos = append(pos, lastPos)
	}
	t.lastPos = pos

}

func (t *TermPosting) nextLog() {
	t.nextLog2()
	for t.lastTimeStamp < t.minTimeStamp && t.lastLogID != NOMOREDOCS {
		t.nextLog2()
	}
}

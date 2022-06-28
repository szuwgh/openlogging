package disk

import (
	"github.com/szuwgh/temsearch/pkg/engine/tem/byteutil"
	"github.com/szuwgh/temsearch/pkg/engine/tem/util"
)

type skiplistWriter struct {
	skipBuf []byteutil.EncBuf
}

type logFreqWriter struct {
	freqBuf   byteutil.EncBuf
	posBuf    byteutil.EncBuf
	lastLogID uint64
	logNum    int
}

func (s *skiplistWriter) addLogID(logID uint64) error {
	return nil
}

func (f *logFreqWriter) addLogID(logID uint64, pos []int) error {
	util.Assert(logID > f.lastLogID, "current logid small than last logid")
	if len(pos) == 1 {
		f.freqBuf.PutUvarint64((logID-f.lastLogID)<<1 | 1)
	} else {
		f.freqBuf.PutUvarint64((logID - f.lastLogID) << 1)
		f.freqBuf.PutUvarint(len(pos))
	}
	lastp := 0
	for _, p := range pos {
		f.posBuf.PutUvarint(p - lastp)
		lastp = p
	}
	f.logNum++
	return nil
}

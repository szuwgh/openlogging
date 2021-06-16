package lsm

import (
	"testing"
)

func Test_TermLog(t *testing.T) {
	// termLog := newTermLog()
	// skipBytes := [][]byte{
	// 	[]byte{3, 2, 2, 6, 5, 5, 9, 8, 8, 12, 11, 11},
	// 	[]byte{9, 9},
	// 	[]byte{},
	// 	[]byte{},
	// 	[]byte{},
	// 	[]byte{},
	// }
	// logFreqBytes := []byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	// posBytes := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	// termLog.skipReader = make([]blockReader, len(skipBytes))
	// for i := 0; i < FreqSkipListLevel; i++ {
	// 	termLog.skipReader[i] = blockReader{data: skipBytes[i], free: len(skipBytes[i])}
	// }
	// termLog.logFreqReader = blockReader{data: logFreqBytes, free: len(logFreqBytes)}
	// termLog.posReader = blockReader{data: posBytes, free: len(posBytes)}

	// termLog.skipTo(1)
	// fmt.Println(termLog.logID())
	// termLog.skipTo(7)
	// fmt.Println(termLog.logID())
	// termLog.skipTo(15)
	// fmt.Println(termLog.logID())
}

func Test_blockDirs(t *testing.T) {
	dirs, _ := blockDirs("E:\\goproject\\temsearch2\\src\\data")
	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			continue
		}
	}
}

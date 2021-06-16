package disk

import "testing"

func Test_NextSequenceExtFile(t *testing.T) {
	s, i, err := nextSequenceExtFile("../fileutil", ".txt")
	t.Log(s, i, err)
}

func Test_Pos(t *testing.T) {
	seq := 5 << 32
	pos := seq | 123456
	t.Log(pos)

	seq0 := int(pos >> 32)
	off := int((pos << 32) >> 32)

	t.Log(seq0, off)
}

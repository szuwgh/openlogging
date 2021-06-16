package fileutil

import "testing"

func Test_ReadDirFromExt(t *testing.T) {
	names, _ := ReadDirFromExt("./", ".txt")
	for i := range names {
		t.Log(names[i])
	}
}

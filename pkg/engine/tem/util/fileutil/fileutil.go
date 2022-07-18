package fileutil

import (
	"os"
	"sort"
	"strings"
)


// Fsync is a wrapper around file.Sync(). Special handling is needed on darwin platform.
func Fsync(f *os.File) error {
	return f.Sync()
}

// ReadDir returns the filenames in the given directory in sorted order.
func ReadDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func ReadDirFromExt(dirpath, ext string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names0, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var names1 []string
	if ext != "" {
		for _, n := range names0 {
			if strings.HasSuffix(n, ext) {
				names1 = append(names1, n)
			}
		}
	}
	sort.Strings(names1)
	return names1, nil
}

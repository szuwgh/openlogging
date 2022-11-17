package logfile

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIO(fName string, fsize int64) *Mmap {
	return nil
}

func (f *FileIO) Write(b []byte, offset int64) (int, error) {
	return 0, nil
}

func (f *FileIO) Read(offset int64) ([]byte, error) {
	return nil, nil
}

func (f *FileIO) Sync() error {
	return nil
}

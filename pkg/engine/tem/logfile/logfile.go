package logfile

import (
	"errors"
	"os"
)

// ErrInvalidFsize invalid file size.
var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

//ErrOutOfFile file out of range
var ErrOutOfFile = errors.New("file out of range")

type LogFile interface {
	Write(b []byte, offset int64) (int, error)

	Read(b []byte, offset int64) (int, error)

	Sync() error

	Close() error
}

func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}

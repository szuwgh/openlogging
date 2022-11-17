package fileutil

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func Mmap(f *os.File, writable bool, length int) ([]byte, error) {
	// anonymous mapping
	if f == nil {
		return unix.Mmap(-1, 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	}
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	mmap, err := unix.Mmap(int(f.Fd()), 0, length, mtype, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return mmap, nil
}

func Munmap(b []byte) (err error) {
	return unix.Munmap(b)
}

func Msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}

// madviseWillNeed gives the kernel the mmap madvise value MADV_WILLNEED, hinting
// that we plan on using the provided buffer in the near future.
func madviseWillNeed(b []byte) error {
	return madvise(b, syscall.MADV_WILLNEED)
}

func madviseDontNeed(b []byte) error {
	return madvise(b, syscall.MADV_DONTNEED)
}

// From: github.com/boltdb/bolt/bolt_unix.go
func madvise(b []byte, advice int) (err error) {
	return unix.Madvise(b, advice)
}

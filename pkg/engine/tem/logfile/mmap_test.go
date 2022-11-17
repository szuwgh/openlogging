package logfile

import (
	"fmt"
	"testing"
)

func Test_Mmap_write(t *testing.T) {
	m, err := NewMmap("./test.file", 100)
	if err != nil {
		t.Fatal(err)
	}
	n, err := m.Write([]byte("aabbcc"), 1)
	if err != nil {
		t.Fatal(err)
	}
	m.Sync()
	n, err = m.Write([]byte("112233"), int64(n))
	if err != nil {
		t.Fatal(err)
	}
	m.Sync()
	m.Close()
}

func Test_Mmap_read(t *testing.T) {
	m, err := NewMmap("./test.file", 100)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	n, err := m.Read(b, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(b[:n], n)
	n, err = m.Read(b, int64(n))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(b[:n], n)
}

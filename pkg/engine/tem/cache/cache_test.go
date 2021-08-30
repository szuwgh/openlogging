package cache

import (
	"fmt"
	"testing"
	"time"
)

type block struct {
	data []byte
}

func (b *block) Release() {
	b.data = nil
}

func Test_Cache(t *testing.T) {
	cacher := NewLRU(20)
	bcache := NewCache(cacher)
	h := bcache.Get(1, 1, func() (size int, value Value) {
		b := &block{}
		data := []byte{'1', '3', '4'}
		b.data = data
		return cap(data), b
	})
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(1, b.data)
		h.Release() //释放
	}

	bcache.Get(1, 2, func() (size int, value Value) {
		b := &block{}
		data := []byte{'2', '3', '4'}
		b.data = data
		return cap(data), b
	})
	bcache.Get(1, 3, func() (size int, value Value) {
		b := &block{}
		data := []byte{'3', '3', '4'}
		b.data = data
		return cap(data), b
	})
	bcache.Get(1, 4, func() (size int, value Value) {
		b := &block{}
		data := []byte{'4', '3', '4'}
		b.data = data
		return cap(data), b
	})

	// bcache.Get(1, 5, func() (size int, value Value) {
	// 	b := &block{}
	// 	data := []byte{'5', '3', '4'}
	// 	b.data = data
	// 	return cap(data), b
	// })
	// bcache.Get(1, 6, func() (size int, value Value) {
	// 	b := &block{}
	// 	data := []byte{'6', '3', '4'}
	// 	b.data = data
	// 	return cap(data), b
	// })
	// bcache.Get(1, 7, func() (size int, value Value) {
	// 	b := &block{}
	// 	data := []byte{'7', '3', '4'}
	// 	b.data = data
	// 	return cap(data), b
	// })

	fmt.Println(bcache.cacher.Used(), bcache.cacher.Capacity())

	bcache.grow()

	var key uint64 = 1
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}
	key = 2
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 3
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 4
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 5
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	// h = bcache.Get(1, 5, nil)
	// if h != nil {
	// 	b := h.Value().(*block)
	// 	fmt.Println(b.data)
	// }

	time.Sleep(5 * time.Second)
	fmt.Println("-----------------------------")
	key = 1
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}
	key = 2
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 3
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 4
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	key = 5
	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

	h = bcache.Get(1, key, nil)
	if h != nil {
		b := h.Value().(*block)
		fmt.Println(key, b.data)
	}

}

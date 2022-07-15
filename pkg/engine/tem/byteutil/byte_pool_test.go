package byteutil

import (
	"fmt"
	"testing"
)

func Test_bytePoolWrite(t *testing.T) {
	//BYTE_BLOCK_SIZE = 10
	alloc := NewByteBlockStackAllocator()
	bytePool := NewInvertedBytePool(alloc)
	offset := bytePool.InitBytes(3)

	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA, 0x01} //,
	offset, _ = bytePool.writeBytes(offset, b)

	fmt.Println(offset)
	fmt.Println(bytePool.buffers)
}

func Test_bytePooWriteString(t *testing.T) {
	bytePool := NewInvertedBytePool(nil)
	offset := bytePool.InitBytes(3)
	bytePool.InitBytes(3)
	bytePool.InitBytes(3)
	b := "aa"
	offset, _ = bytePool.WriteString(offset, b)
	fmt.Println(offset)
	// bytePool.writeBytes(offset, store.IntToByte(2))
	// fmt.Println(bytePool.buffer)
}

func Test_bytePooWriteVInt(t *testing.T) {
	bytePool := NewInvertedBytePool(nil)
	offset := bytePool.InitBytes(3)
	offset, _ = bytePool.writeVInt(offset, 123)
	fmt.Println(offset, bytePool.buffers)
	offset, _ = bytePool.writeVInt(offset, 123)
	fmt.Println(offset, bytePool.buffers)
	offset, _ = bytePool.writeVInt(offset, 123)
	fmt.Println(offset, bytePool.buffers)
	offset, _ = bytePool.writeVInt(offset, 123)
	fmt.Println(offset, bytePool.buffers)
}

func Test_slice(t *testing.T) {

	slice := make([]byte, 4)
	offset := 4294967294
	for i := 0; i < 4; i++ {
		slice[i] = byte(offset >> uint(8*(3-i)))
	}
	fmt.Println(slice)
}

func Test_Mod(t *testing.T) {
	a := 159770
	b := 1 << 15
	fmt.Println(b)
	fmt.Println(a % b)
	fmt.Println(a & (b - 1))
}

func Test_initBytes(t *testing.T) {
	bytePool := NewInvertedBytePool(nil)
	bytePool.InitBytes(3)
	fmt.Println(bytePool.buffer)
}

func Test_bytePoolWrite2(t *testing.T) {
	alloc := NewByteBlockStackAllocator()
	bytePool := NewForwardBytePool(alloc)
	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA}
	n1 := bytePool.useByteOffset
	fmt.Println("offset", bytePool.useByteOffset)
	bytePool.writeMsg(b)
	n2 := bytePool.useByteOffset
	fmt.Println("offset", bytePool.useByteOffset)
	bytePool.writeMsg(b)
	n3 := bytePool.useByteOffset
	fmt.Println("offset", bytePool.useByteOffset)
	bytePool.writeMsg(b)
	fmt.Println(bytePool.buffers)
	fmt.Println(bytePool.endIndex)
	reader := NewForwardBytePoolReader(bytePool)
	reader.Init(n1)
	fmt.Println(reader.ReadString())
	reader.Init(n2)
	fmt.Println(reader.ReadString())
	reader.Init(n3)
	fmt.Println(reader.ReadString())
	iter := bytePool.Iterator()
	for iter.NextBuffer() {
		for iter.NextBytes() {
			fmt.Println(iter.Bytes())
		}
	}
	// bytePool.rotateMem()
}

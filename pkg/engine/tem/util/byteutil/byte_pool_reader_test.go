package byteutil

import (
	"fmt"
	"testing"
)

func Test_bytePoolReader(t *testing.T) {
	bytePool := NewInvertedBytePool(nil)
	offset := bytePool.InitBytes(3)
	byteStart := offset
	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA}
	offset, _ = bytePool.writeBytes(offset, b)
	fmt.Println("offset", offset)
	fmt.Println(bytePool.buffers)
	reader := NewInvertedBytePoolReader(bytePool, 0, 3)
	reader.initReader(byteStart, uint64(len(b)))
	x := reader.readBytes(20)
	fmt.Println(x)
}

func Test_bytePoolReadVInt(t *testing.T) {
	alloc := NewByteBlockStackAllocator()
	bytePool := NewInvertedBytePool(alloc)
	//bytePool.newBytes2(8)
	offset := bytePool.InitBytes(6)
	byteStart := offset
	for i := 0; i < 30; i++ {
		offset, _ = bytePool.writeVInt(offset, 163850)
	}

	fmt.Println(bytePool.buffers)
	reader := NewInvertedBytePoolReader(bytePool, 0, 3)
	reader.initReader(byteStart, offset)
	for i := 0; i < 30; i++ {
		fmt.Println(reader.readVInt())
	}
}

func Test_bytePoolReadString(t *testing.T) {
	bytePool := NewInvertedBytePool(nil)
	offset := bytePool.InitBytes(6)
	byteStart := offset

	offset, _ = bytePool.WriteString(offset, "aaaabbbbbbbbbbb")
	fmt.Println(bytePool.buffer)
	reader := NewInvertedBytePoolReader(bytePool, 0, 3)
	reader.initReader(byteStart, offset)
	fmt.Println(reader.readString())
	reader.initReader(byteStart, offset)
	fmt.Println(reader.readString())
}

func Test_bytePoolGetBlock(t *testing.T) {
	//BYTE_BLOCK_SIZE = 128
	alloc := NewByteBlockStackAllocator()
	bytePool := NewInvertedBytePool(alloc)
	offset := bytePool.InitBytes(6)
	byteStart := offset
	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA,
	}
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	offset, _ = bytePool.writeBytes(offset, b)
	for i := range bytePool.buffers {
		fmt.Println(bytePool.buffers[i])
	}

	reader := NewInvertedBytePoolReader(bytePool, 0, 3)
	reader.initReader(byteStart, offset)

	for reader.Next() {
		fmt.Println(reader.Block())
	}

}

func Test_bytePoolSnapBlock(t *testing.T) {
	//	BYTE_BLOCK_SIZE = 32
	alloc := NewByteBlockStackAllocator()
	bytePool := NewInvertedBytePool(alloc)
	offset := bytePool.InitBytes(6)
	byteStart := offset
	b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xA}
	offset, _ = bytePool.writeBytes(offset, b)
	fmt.Println(bytePool.buffers)

	reader := NewInvertedBytePoolReader(bytePool, 0, 3)
	//reader.initReader(byteStart, len(b))

	sb := NewSnapBlock(byteStart, offset, uint64(len(b)), reader)
	var err error
	var res byte
	var i int
	for err == nil {
		res, err = sb.ReadByte()
		fmt.Print(res, " ")
		i++
		if i == 100 {
			break
		}
	}
	fmt.Println(sb.cur.data)
	sb.Seek(1)
	res, _ = sb.ReadByte()
	fmt.Print(res, " ")
	sb.Seek(4)
	res, _ = sb.ReadByte()
	fmt.Print(res, " ")
	sb.Seek(19)
	res, _ = sb.ReadByte()
	fmt.Print(res, " ")
	sb.Seek(0)
	res, _ = sb.ReadByte()
	fmt.Print(res, " ")
}

func Test_bytereader2(t *testing.T) {

	alloc := NewByteBlockStackAllocator()

	bytePool := NewForwardBytePool(alloc)
	b := "woshishui3334456777771234567890123456788"
	//bytePool.writeVInt(5)
	bytePool.writeString(b)
	fmt.Println(bytePool.buffers)

	reader := NewForwardBytePoolReader(bytePool)
	///reader.Init(0)
	//fmt.Println(reader.readVInt())
	//fmt.Println(string(reader.ReadString()))
	reader.InitReader(0)
	for reader.Next() {
		fmt.Println(string(reader.Block()))
	}

}

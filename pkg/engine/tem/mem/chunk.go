package mem

type chunk struct {
	data []byte
}

// func (c *chunk) writeByte(i uint64, b byte) uint64 {
// 	//m, n := turn(i)
// 	buffer := c.data.buffers[m]
// 	if buffer[n] == 0 {
// 		buffer[n] = b
// 	} else {
// 		//申请新的内存
// 		i = ib.allocBytes(buffer[n:n+4], buffer[n])
// 		m, n = turn(i)
// 		ib.buffers[m][n] = b
// 	}
// 	i++
// 	ib.byteSize++
// 	return i
// }

func (c *chunk) putVInt() {

}

type labelChunk struct {
	log []byte
}

type temChunk struct {
	logFreq []byte
	skip    [][]byte
	pos     []byte
}

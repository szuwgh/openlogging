package mem

type labelChunk struct {
	log []byte
}

type temChunk struct {
	logFreq []byte
	skip    [][]byte
	pos     []byte
}

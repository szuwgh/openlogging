package mem

type raxNode struct {
	iskey   uint32
	isNull  uint32
	iscompr uint32
	size    uint32
}

//redis radix tree
type radixTree struct {
	head *raxNode
}

func (rax *radixTree) Insert(key []byte, byteStart int) int {

	return rax.insert(key, byteStart)
}

func (rax *radixTree) insert(key []byte, byteStart int) int {
	return 0
}

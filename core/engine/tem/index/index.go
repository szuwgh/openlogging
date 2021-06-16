package index

type Iterator interface {
	Next() bool
	Value() interface{}
	Key() []byte
	IsTag() bool
}

//倒排表
type Index interface {
	Find([]byte) (interface{}, bool)
	Insert([]byte, interface{})
	Iterator() Iterator
	Free() error
}

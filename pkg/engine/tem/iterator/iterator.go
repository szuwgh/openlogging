package iterator

//Iterator 迭代器
type Iterator interface {
	First() bool
	Next() bool
	Key() []byte
}

type SingleIterator interface {
	Iterator
	Value() []byte
}

type IteratorTable interface {
	SingleIterator
	Iters() []SingleIterator
}

type IteratorIndex interface {
	SingleIterator
	Get() SingleIterator
}

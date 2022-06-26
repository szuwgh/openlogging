package chunks

// Postings provides iterative access over a postings list.
type Postings interface {
	// Next advances the iterator and returns true if another value was found.
	Next() bool

	// Seek advances the iterator to value v or greater and returns
	// true if a value was found.
	Seek(v uint64) bool

	// At returns the value at the current iterator position.
	At() (int64, uint64, []int)

	// Err returns the last error of the iterator.
	Err() error
}

type ChunkReader interface {
	ReadChunk(bool, ...uint64) ChunkEnc
}

type Chunk interface {
	MinTime() int64
	MaxTime() int64
	ChunkEnc(bool, ChunkReader) ChunkEnc
}

type ChunkEnc interface {
	Bytes() [][]byte
	Iterator(mint, maxt int64) Postings
}

// errPostings is an empty iterator that always errors.
type errPostings struct {
	err error
}

func (e errPostings) Next() bool                 { return false }
func (e errPostings) Seek(uint64) bool           { return false }
func (e errPostings) At() (int64, uint64, []int) { return 0, 0, nil }
func (e errPostings) Err() error                 { return e.err }

var EmptyPostings = errPostings{}

func Intersect(its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings
	}
	if len(its) == 1 {
		return its[0]
	}
	l := len(its) / 2
	return newIntersectPostings(Intersect(its[:l]...), Intersect(its[l:]...))
}

type intersectPostings struct {
	a, b     Postings
	aok, bok bool
	cur      uint64
	t        int64
	pos      []int
}

func newIntersectPostings(a, b Postings) *intersectPostings {
	return &intersectPostings{a: a, b: b}
}

func (it *intersectPostings) At() (int64, uint64, []int) {
	return it.t, it.cur, it.pos
}

func (it *intersectPostings) doNext(t int64, id uint64, pos []int) bool {

	for {
		if !it.b.Seek(id) {
			return false
		}
		if _, vb, _ := it.b.At(); vb != id {
			if !it.a.Seek(vb) {
				return false
			}
			t, id, pos = it.a.At()
			if vb != id {
				continue
			}
		}
		it.cur = id
		it.t = t
		it.pos = pos
		return true
	}
}

func (it *intersectPostings) Next() bool {
	if !it.a.Next() {
		return false
	}
	return it.doNext(it.a.At())
}

func (it *intersectPostings) Seek(id uint64) bool {
	if !it.a.Seek(id) {
		return false
	}
	return it.doNext(it.a.At())
}

func (it *intersectPostings) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

func Merge(its ...Postings) Postings {
	if len(its) == 0 {
		return nil
	}
	if len(its) == 1 {
		return its[0]
	}
	l := len(its) / 2
	return newMergedPostings(Merge(its[:l]...), Merge(its[l:]...))
}

type mergedPostings struct {
	a, b        Postings
	initialized bool
	aok, bok    bool
	cur         uint64
	t           int64
	pos         []int
}

func NewMergedPostings(a, b Postings) *mergedPostings {
	return newMergedPostings(a, b)
}

func newMergedPostings(a, b Postings) *mergedPostings {
	return &mergedPostings{a: a, b: b}
}

func (it *mergedPostings) At() (int64, uint64, []int) {
	return it.t, it.cur, it.pos
}

func (it *mergedPostings) Next() bool {
	if !it.initialized {
		it.aok = it.a.Next()
		it.bok = it.b.Next()
		it.initialized = true
	}

	if !it.aok && !it.bok {
		return false
	}

	if !it.aok {
		it.t, it.cur, it.pos = it.b.At()
		it.bok = it.b.Next()
		return true
	}
	if !it.bok {
		it.t, it.cur, it.pos = it.a.At()
		it.aok = it.a.Next()
		return true
	}

	ta, acur, apos := it.a.At()
	tb, bcur, bpos := it.b.At()

	if acur < bcur {
		it.cur = acur
		it.t = ta
		it.pos = apos
		it.aok = it.a.Next()
	} else if acur > bcur {
		it.cur = bcur
		it.t = tb
		it.pos = bpos
		it.bok = it.b.Next()
	} else {
		it.cur = acur
		it.t = ta
		it.pos = apos
		it.aok = it.a.Next()
		it.bok = it.b.Next()
	}
	return true
}

func (it *mergedPostings) Seek(id uint64) bool {
	if it.cur >= id {
		return true
	}

	it.aok = it.a.Seek(id)
	it.bok = it.b.Seek(id)
	it.initialized = true

	return it.Next()
}

func (it *mergedPostings) Err() error {
	if it.a.Err() != nil {
		return it.a.Err()
	}
	return it.b.Err()
}

// // Iterator is a simple iterator that can only get the next value.
// type Iterator interface {
// 	At() (int64, uint64)
// 	Err() error
// 	Next() bool
// }

package posting

import "testing"

func Test_Merge(t *testing.T) {
	a := []uint64{1, 2, 3, 4, 5, 6, 7}
	b := []uint64{2, 3, 4, 5, 6, 7, 8}
	c := []uint64{3, 4, 5, 6, 7, 9}

	posting := newMergedPostings(NewListPostings(a), NewListPostings(b))
	p := Merge(posting, NewListPostings(c))
	for p.Next() {
		t.Log(p.At())
	}

}

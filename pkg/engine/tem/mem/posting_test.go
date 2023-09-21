package mem

import (
	"fmt"
	"testing"

	"github.com/szuwgh/hawkobserve/pkg/engine/tem/posting"
	"github.com/szuwgh/hawkobserve/pkg/temql"
)

func Test_tagIter(t *testing.T) {
	// tag := NewDefalutTagGroup()
	// tag.Set("a", nil)
	// tag.Set("c", nil)
	// tag.Set("e", nil)
	// tag.Set("b", nil)
	// iter := tag.Iterator()
	// for iter.Next() {
	// 	fmt.Println(string(iter.Key()), iter.Value())
	// }
}

type IndexMap map[string][]uint64

func Test_PostingIntersect(t *testing.T) {
	p1 := posting.NewListPostings([]uint64{1, 3, 6, 9, 11, 15, 17})
	p2 := posting.NewListPostings([]uint64{2, 3, 7, 9, 10, 15, 17})
	p := posting.Intersect(p1, p2)
	for p.Next() {
		fmt.Println(p.At())
	}
}

func Test_QueryTerm(t *testing.T) {

	expr := temql.ParseExpr(`a`)
	e := expr.(*temql.VectorSelector)
	//v.Print()
	im := make(IndexMap)
	im["a"] = []uint64{1, 3, 6, 9, 11, 15, 17}
	im["b"] = []uint64{2, 3, 7, 9, 10, 15, 17}
	im["c"] = []uint64{2, 3, 7, 9}

	p := testQueryTerm(e.Expr, im)
	for p.Next() {
		fmt.Println(p.At())
	}

}

func testQueryTerm(e temql.Expr, postingList IndexMap) posting.Postings {
	switch e.(type) {
	case *temql.TermBinaryExpr:
		expr := e.(*temql.TermBinaryExpr)
		p1 := testQueryTerm(expr.LHS, postingList)
		p2 := testQueryTerm(expr.RHS, postingList)
		switch expr.Op {
		case temql.LAND:
			return posting.Intersect(p1, p2)
		case temql.LOR:
			return posting.Merge(p1, p2)
		}
	case *temql.TermExpr:
		e := e.(*temql.TermExpr)
		pointer, _ := postingList[e.Name]
		if pointer == nil {
			return posting.EmptyPostings
		}
		//termList := pointer.(*TermPosting)
		return posting.NewListPostings(pointer)
	}
	return nil
}

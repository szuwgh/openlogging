package parse

import "github.com/sophon-lab/temsearch/pkg/temql/labels"

type Node interface {
	// String representation of the node that returns the given node when parsed
	// as part of a valid query.
	String() string
}

type VectorSelector struct {
	Name string

	LabelMatchers []*labels.Matcher
}

func (*VectorSelector) String() string {
	return "VectorSelector"
}

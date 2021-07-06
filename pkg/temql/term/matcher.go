package term

type MatchType int

const (
	MatchAnd MatchType = iota
	MatchOr
)

// type Matcher struct {
// 	Type  MatchType
// 	Name1 string
// 	Name2 string
// }

type Matcher struct {
	Left *Matcher
	Type MatchType

	Name1 string
	Name2 string
}

// NewMatcher returns a matcher object.
func NewMatcher(v *Matcher, t MatchType, n1, n2 string) *Matcher {
	m := &Matcher{
		Type:  t,
		Name1: n1,
		Name2: n2,
	}
	return m
}

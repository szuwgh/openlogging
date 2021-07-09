package labels

import (
	"strings"
)

type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

type Matcher struct {
	Type  MatchType
	Name  string
	Value string
}

// NewMatcher returns a matcher object.
func NewMatcher(t MatchType, n, v string) *Matcher {
	m := &Matcher{
		Type:  t,
		Name:  n,
		Value: strings.Trim(v, `"`),
	}
	return m
}

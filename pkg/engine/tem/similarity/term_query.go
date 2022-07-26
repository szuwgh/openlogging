package similarity

type TermQuery struct {
}

func (t *TermQuery) sumOfSquaredWeights() float64 {
	return 0.0
}

func (t *TermQuery) getBoost() float64 {
	return 1.0
}

type TermWeight struct {
	idf float64
}

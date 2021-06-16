package tokenizer

type Token struct {
	//分词在文本起始的位置
	Start int
	//分词在文本末尾的位置
	End int
	//分词获得的词语
	Term string
	//词语标注
	Position  int
	Frequency int
}

type Tokens []*Token

type TokenFreq struct {
	//Term      []byte
	Locations []*TokenLocation
	Frequency int
}

func (tf *TokenFreq) GetFrequency() int {
	return tf.Frequency
}

type TokenLocation struct {
	Start    int
	End      int
	Position int
}

type TokenFrequencies []*TokenFreq

func (TokenFrequencies) Sort() {

}

package analysis

import (
	"github.com/szuwgh/athena/pkg/tokenizer"
	"github.com/yanyiwu/gojieba"
)

type Analyzer struct {
	t tokenizer.Tokenizer
}

func NewAnalyzer(tokenizerType string) *Analyzer {
	a := &Analyzer{}
	var err error
	config := make(map[string]interface{})
	config["dict_path"] = gojieba.DICT_PATH
	config["hmmpath"] = gojieba.HMM_PATH
	config["userdictpath"] = gojieba.USER_DICT_PATH
	config["idf"] = gojieba.IDF_PATH
	config["stop_words"] = gojieba.STOP_WORDS_PATH
	a.t, err = tokenizer.RegistryInstance.NewTokenizer(tokenizerType, config)
	if err != nil {
		return nil
	}
	return a

}

//分析
func (a *Analyzer) Analyze(input []byte) tokenizer.Tokens {
	tokens := a.t.Tokenize(input)
	return tokens
}

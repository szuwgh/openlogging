package gojieba

import (
	"errors"

	"github.com/sophon-lab/temsearch/core/tokenizer"

	"github.com/yanyiwu/gojieba"
)

func init() {
	tokenizer.RegiterConstructor("gojieba", NewTokenizer)
}

type JiebaTokenizer struct {
	handle *gojieba.Jieba
}

func NewTokenizer(config map[string]interface{}) (tokenizer.Tokenizer, error) {
	dictPath, ok := config["dict_path"].(string)
	if !ok {
		return nil, errors.New("config dictpath not found")
	}
	hmmPath, ok := config["hmmpath"].(string)
	if !ok {
		return nil, errors.New("config hmmpath not found")
	}
	userDictPath, ok := config["userdictpath"].(string)
	if !ok {
		return nil, errors.New("config userdictpath not found")
	}
	idf, ok := config["idf"].(string)
	if !ok {
		return nil, errors.New("config idf not found")
	}
	stopWords, ok := config["stop_words"].(string)
	if !ok {
		return nil, errors.New("config stop_words not found")
	}
	return newTokenizer(dictPath, hmmPath, userDictPath, idf, stopWords), nil
}

func newTokenizer(dictPath, hmmPath, userDictPath, idf, stopWords string) *JiebaTokenizer {
	return &JiebaTokenizer{
		handle: gojieba.NewJieba(dictPath, hmmPath, userDictPath, idf, stopWords),
	}
}

//tokenize
func (t *JiebaTokenizer) Tokenize(content []byte) tokenizer.Tokens {
	result := make(tokenizer.Tokens, 0)
	pos := 1
	words := t.handle.Tokenize(string(content), gojieba.SearchMode, true)
	for _, word := range words {
		token := tokenizer.Token{
			Term:     word.Str,
			Start:    word.Start,
			End:      word.End,
			Position: pos,
		}
		result = append(result, &token)
		pos++
	}
	return result
}

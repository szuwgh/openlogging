package gojieba

import (
	"fmt"
	"strings"
	"testing"

	gojb "github.com/yanyiwu/gojieba"
)

func Test_gojieba(t *testing.T) {
	var s string
	var words []string
	use_hmm := true
	x := gojb.NewJieba()
	defer x.Free()

	s = "我来到北京清华大学"
	words = x.CutAll(s)
	fmt.Println(s)
	fmt.Println("全模式:", strings.Join(words, "/"))

	words = x.Cut(s, use_hmm)
	fmt.Println(s)
	fmt.Println("精确模式:", strings.Join(words, "/"))
	s = "比特币"
	words = x.Cut(s, use_hmm)
	fmt.Println(s)
	fmt.Println("精确模式:", strings.Join(words, "/"))

	x.AddWord("比特币")
	s = "比特币"
	words = x.Cut(s, use_hmm)
	fmt.Println(s)
	fmt.Println("添加词典后,精确模式:", strings.Join(words, "/"))

	s = "他来到了网易杭研大厦"
	words = x.Cut(s, use_hmm)
	fmt.Println(s)
	fmt.Println("新词识别:", strings.Join(words, "/"))

	s = "小明硕士毕业于中国科学院计算所，后在日本京都大学深造"
	words = x.CutForSearch(s, use_hmm)
	fmt.Println(s)
	fmt.Println("搜索引擎模式:", strings.Join(words, "/"))

	s = "长春市长春药店"
	words = x.Tag(s)
	fmt.Println(s)
	fmt.Println("词性标注:", strings.Join(words, ","))

	s = "区块链"
	words = x.Tag(s)
	fmt.Println(s)
	fmt.Println("词性标注:", strings.Join(words, ","))

	s = "长江,百度搜索是最差劲的，长江"
	words = x.CutForSearch(s, !use_hmm)
	fmt.Println(s)
	fmt.Println("搜索引擎模式:", strings.Join(words, "/"))

	wordinfos := x.Tokenize(s, gojb.SearchMode, !use_hmm)
	fmt.Println(s)
	fmt.Println("Tokenize:(搜索引擎模式)", wordinfos)

	wordinfos = x.Tokenize(s, gojb.DefaultMode, !use_hmm)
	fmt.Println(s)
	fmt.Println("Tokenize:(默认模式)", wordinfos)

	keywords := x.ExtractWithWeight(s, 5)
	fmt.Println("Extract:", keywords)
}

func Test_gojieba_tokenizer(t *testing.T) {
	tokenizer, _ := NewTokenizer(map[string]interface{}{
		"dict_path":      gojb.DICT_PATH,
		"hmm_path":       gojb.HMM_PATH,
		"user_dict_path": gojb.USER_DICT_PATH,
		"idf":            gojb.IDF_PATH,
		"stop_words":     gojb.STOP_WORDS_PATH,
		"type":           "gojieba",
	})
	tokens := tokenizer.Tokenize([]byte("长江,百度搜索是最差劲的，长江"))
	for _, v := range tokens {
		fmt.Println(v)
	}
}

func Test_tokenizer(t *testing.T) {
	var s string
	//var words []string
	s = "2022年 03月 17日 星期四 17:59:21 CST 0649b1d755"
	use_hmm := true
	x := gojb.NewJieba()
	defer x.Free()
	wordinfos := x.Tokenize(s, gojb.SearchMode, use_hmm)
	fmt.Println(s)
	fmt.Println("Tokenize:(搜索引擎模式)", wordinfos)
}

package generate

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
)

var (
	lookup = map[string]int{
		// Ranges calculated from data found at
		// http://en.wikipedia.org/wiki/Letter_frequency
		"a": 8167, "b": 9659, "c": 12441, "d": 16694, "e": 29396, "f": 31624, "g": 33639, "h": 39733,
		"i": 46699, "j": 46852, "k": 47624, "l": 51649, "m": 54055, "n": 60804, "o": 68311, "p": 70240,
		"q": 70335, "r": 76322, "s": 82649, "t": 91705, "u": 94463, "v": 95441, "w": 97801, "x": 97951,
		"y": 99925, "z": 100000,
	}
)

type RandomChar struct {
	Char     string
	Charfreq int
	Chance   string
}

type WordLength struct {
	length int
	chance string
}

type WorldList struct {
	Length  int
	Chance  string
	Word    string
	CharMap []RandomChar
}

// 判断元素是否在数组中
func isExist(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

//判断是奇数
func isOdd(num int) bool {
	if num%2 == 0 {
		return false
	}
	return true
}

// 返回随机数
func randomNum() int {
	var n uint32
	const max = 100000
	binary.Read(rand.Reader, binary.LittleEndian, &n)
	if n < max {
		return int(n)
	} else {
		return int(n / max)
	}
}

// 转换大小写
func mixCase(s string) string {
	var (
		bufferTmp bytes.Buffer
	)
	for i := 0; i < len(s); i++ {
		random := randomNum()
		str := ""
		if isOdd(random) {
			str = strings.ToTitle(string(s[i]))
		} else {
			str = strings.ToLower(string(s[i]))
		}
		bufferTmp.WriteString(str)
	}
	return bufferTmp.String()
}

// 选择随机数字符
func randomAtoZ(lookup map[string]int) func() RandomChar {
	return func() RandomChar {
		r := RandomChar{}
		prev := 0
		random := randomNum()
		for key := range lookup {
			charfreq := lookup[key]
			chance := strconv.Itoa((charfreq-prev)/1000) + "%"
			if random < charfreq {
				r.Char = key
				r.Charfreq = charfreq
				r.Chance = chance
				break
			}
			prev = charfreq
		}
		//fmt.Println(r)
		return r
	}
}

// 随机长度
func randomWordLength() WordLength {
	var (
		wordfrequency []int
		total         int
		lookupList    map[int]int
	)
	lookupList = make(map[int]int)
	wl := WordLength{}
	total = 0
	const max = 100000
	// word length frequency in the english language from 1-19 characters
	percentages := [...]float64{0.1, 0.6, 2.6, 5.2, 8.5, 12.2, 14.0, 14.0, 12.6, 10.1, 7.5, 5.2, 3.2, 2.0, 1.0, 0.6, 0.3, 0.2, 0.1}
	lengthJ := len(percentages)
	for j := 0; j < lengthJ; j++ {
		percent := percentages[j]
		d1, _ := decimal.NewFromFloat(percent).Div(decimal.NewFromFloat(float64(100))).Float64()
		d2, _ := decimal.NewFromFloat(d1).Mul(decimal.NewFromFloat(float64(max))).Float64()
		amount := total + int(d2)
		wordfrequency = append(wordfrequency, amount)
		total = amount
	}
	random := randomNum()
	// 循环赋值
	for n := 0; n < len(wordfrequency); n++ {
		lookupList[n+1] = wordfrequency[n]
	}
	prev := 0
	for key := range lookupList {
		lengthfreq := lookupList[key]
		chance := strconv.Itoa((lengthfreq-prev)/1000) + "%"
		if random < lengthfreq {
			wl.chance = chance
			wl.length = key
			break
		}
		prev = lengthfreq
	}
	return wl
}

// GenRandomWorld returns a random word by convention,
// The length must be between 0 and 19.
// The model must be : none,upper,lower,mix,title
// none: String lowercase
// upper: String uppercase
// lower: String lowercase
// mix: String random case
// title: String First case
func GenRandomWorld(length int, model string) (WorldList, error) {
	var (
		buffer         bytes.Buffer
		randomCharList []RandomChar
		lengthFlag     WordLength
	)
	models := []string{"none", "upper", "lower", "mix", "title"}

	if !isExist(models, model) {
		return WorldList{}, errors.New("The model must be : none,upper,lower,mix,title")
	}

	if length > 19 || length < 0 {
		return WorldList{}, errors.New("The length must be between 0 and 19")
	}

	for {
		lengthFlag = randomWordLength()
		if length == 0 {
			break
		}
		if lengthFlag.length == length {
			break
		}
	}

	lengthI := lengthFlag.length
	chance := lengthFlag.chance
	world := ""
	for i := 1; i <= lengthI; i++ {
		ran := randomAtoZ(lookup)
		ranItem := ran()
		buffer.WriteString(ranItem.Char)
		randomCharList = append(randomCharList, ranItem)
	}
	switch {
	case model == "upper":
		// 大写字符串
		world = strings.ToTitle(buffer.String())
	case model == "title":
		// 首字母大写
		world = strings.Title(buffer.String())
	case model == "mix":
		// 大小写混合模式
		world = mixCase(buffer.String())
	default:
		world = buffer.String()

	}
	w := WorldList{
		Length:  lengthI,
		Chance:  chance,
		Word:    world,
		CharMap: randomCharList,
	}
	return w, nil
}

// none
func GenRandomNone(length int) (WorldList, error) {
	return GenRandomWorld(length, "none")
}

// lower
func GenRandomLower(length int) (WorldList, error) {
	return GenRandomWorld(length, "lower")
}

// title
func GenRandomTitle(length int) (WorldList, error) {
	return GenRandomWorld(length, "title")
}

// mix
func GenRandomMix(length int) (WorldList, error) {
	return GenRandomWorld(length, "mix")
}

// upper
func GenRandomUpper(length int) (WorldList, error) {
	return GenRandomWorld(length, "upper")
}

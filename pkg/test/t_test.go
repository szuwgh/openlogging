package test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"
)

func adder() func(int) int {
	sum := 0
	return func(x int) int {
		sum += x
		return sum
	}
}

func Test_a(t *testing.T) {
	a := adder()
	fmt.Println(a(0), a(1), a(2)) //0 1 3
}

func Test_b(t *testing.T) {
	s := []string{"a", "b", "c"}
	for _, v := range s {
		go func() {
			fmt.Println(v)
		}()
	}
	select {}
	// c
	// c
	// c
}

func test() []func() {
	var s []func()
	for i := 0; i < 3; i++ {
		s = append(s, func() {
			fmt.Println(&i, i)
		})
	}
	return s
}

func Test_c(t *testing.T) {
	for _, f := range test() {
		f()
	}
}

func Test_d(t *testing.T) {
	x, y := 1, 2

	defer func(a int) {
		fmt.Printf("x:%d,y:%d\n", a, y)
	}(x)

	defer func(a int) {
		fmt.Printf("y:%d,y:%d\n", a, y)
	}(y)

	x += 100
	y += 100
	fmt.Println(x, y)
	// 101 102
	// y:2,y:102
	// x:1,y:102
}

func Test_f(t *testing.T) {
	var a string = "a"

	fmt.Println(unsafe.Sizeof(a))
}

func Test_Json(t *testing.T) {
	x := map[string]string{
		"@timestamp":     "November 6th 2018, 15:00:48.474",
		"@version":       "1",
		"CONTAINER_ID":   "c5f028f55eac3b1803349b2b1784931f7ad43981e70a431e069e3ee646b32611",
		"CONTAINER_NAME": "calico-node",
		"NAMESPACE":      "kube-system",
		"POD_NAME":       "calico-node-qx8xl",
		"beat.hostname":  "v-node1-kcstest.sz.kingdee.net",
		"host.name":      "v-node1-kcstest.sz.kingdee.net",
		"log":            "2018-11-06 07:26:59.331 [DEBUG][141] int_dataplane.go 672: Throttle kick received",
		"message":        `{"log":"2018-11-06 07:26:59.331 [DEBUG][141] int_dataplane.go 672: Throttle kick received\n","stream":"stdout","time":"2018-11-06T07:26:59.33229893Z"}`,
	}
	b, _ := json.Marshal(x)
	fmt.Println(string(b))
}

func Test_Login(t *testing.T) {
	Login()

}

func Login() {
	client := &http.Client{}
	//生成要访问的url
	url := "http://localhost:9013/login?tid=200002&gid=8&uid=200006"
	//提交请求
	reqest, err := http.NewRequest("GET", url, nil)

	if err != nil {
		panic(err)
	}
	//处理返回结果
	response, err := client.Do(reqest)
	if err != nil {
		panic(err)
	}
	//defer response.Body.Close()
	b, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(b))
	fmt.Println(response.Cookies())
	AA(response.Cookies())

}

func AA(cookie []*http.Cookie) {
	client := &http.Client{}
	//生成要访问的url
	url := "http://localhost:9002/ajax/monitor/job/alarm_policy?job_id=71"
	//提交请求
	reqest, err := http.NewRequest("GET", url, nil)

	//增加header选项
	cookieMap := make(map[string]*http.Cookie)
	for _, v := range cookie {
		cookieMap[v.Name] = v
	}

	for _, v := range cookieMap {
		reqest.AddCookie(v)
	}

	if err != nil {
		panic(err)
	}
	//处理返回结果
	response, err := client.Do(reqest)
	if err != nil {
		panic(err)
	}
	//defer response.Body.Close()
	b, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(b))
}

func Test_BB(t *testing.T) {
	i := []int{0, 1, 2, 3, 4, 5, 6}
	fmt.Println(i[:2])
	fmt.Println(i[2:5])
}

func dd() int {
	t := 5
	defer func() {
		t = t + 5
	}()
	return t
}

func Test_DD(t *testing.T) {
	fmt.Println(dd())
}

func cc(aa map[string]int) {
	//aa = make(map[string]int)
	aa["aa"] = 1
}

func Test_SS(t *testing.T) {
	var aa map[string]int
	aa = make(map[string]int)
	cc(aa)
	fmt.Println(aa)
}

func Test_Map(t *testing.T) {
	var _map map[string]string
	fmt.Printf("%p\n", &_map)
	_map2(&_map)
	fmt.Println(_map)

}

func _map2(_map *map[string]string) {
	fmt.Printf("%p\n", &_map)
	*_map = make(map[string]string)
	(*_map)["d"] = "d"
}

func Test_hex(t *testing.T) {

	fmt.Println(1 << 15)
}

func jiemi(targetStr string) {
	var b []int32
	for _, v := range targetStr {
		b = append(b, v-1)
	}
	var str2 []string
	for _, v := range b {
		str2 = append(str2, string(v))
	}
	c, _ := hex.DecodeString(strings.Join(str2, ""))
	fmt.Println(string(c))
}

func Test_array(t *testing.T) {
	a := []int{1, 2, 3, 4, 5, 6, 7}
	num := 2
	for i := 0; i < 3; i++ {
		start := i * num
		b := a[start : start+num]
		fmt.Println(b)
	}
	fmt.Println(a[3*num : 3*num+1])
}

func Test_Int(t *testing.T) {
	var a int = 7
	b := int(float32(a) * 1.5)
	fmt.Println(b)
}

func Test_Bytes(t *testing.T) {
	b := make([]byte, 10)
	for i := 0; i < len(b); i++ {
		b[i] = byte(i)
	}
	fmt.Println(b[6:8])
}

func Test_Byte(t *testing.T) {
	a := make([]byte, 2)
	fmt.Printf("%p\n", &a)
	bytese(a)
	fmt.Println(a)
}

func bytese(b []byte) {
	fmt.Printf("%p\n", &b)
	fmt.Printf("%p\n", b)
	b[0] = byte(1)
}

func Test_Mod(t *testing.T) {
	fmt.Println(0 % 5)
}

func Test_Buffer(t *testing.T) {
	b := bytes.Buffer{}
	b.Write([]byte("12345677"))
	b.Truncate(3)
	fmt.Println(string(b.Bytes()))
}

func Test_slice(t *testing.T) {
	var a []int
	a = append(a, 1)
	a = append(a, 2)
	a = append(a, 3)
	fmt.Println(a)
	fmt.Printf("%p\n", &a)
	fmt.Println(len(a))
	fmt.Println(cap(a))
	a = a[:0]
	fmt.Println("---")
	fmt.Println(a)
	fmt.Printf("%p\n", a)
	fmt.Println(len(a))
	fmt.Println(cap(a))

	a = append(a, 1)
	a = append(a, 2)
	a = append(a, 3)
	a = append(a, 4)
	fmt.Println("---")
	fmt.Println(a)
	fmt.Printf("%p\n", a)
	fmt.Println(len(a))
	fmt.Println(cap(a))
	a = append(a, 5)
	fmt.Println("---")
	fmt.Println(a)
	fmt.Printf("%p\n", a)
	fmt.Println(len(a))
	fmt.Println(cap(a))
}

func Test_slice2(t *testing.T) {
	a := make([]int, 4)
	fmt.Println(len(a))
	fmt.Println(cap(a))
}

func Test_map(t *testing.T) {
	var m map[string]int
	_map(m)
	fmt.Println("Test_map", m)
}

func _map(m map[string]int) {
	m = make(map[string]int)
	m["a"] = 1
	fmt.Println("_map", m)
}

func Test_map2(t *testing.T) {
	a := []int{1, 3, 4, 5}
	var b uint32 = 2
	fmt.Println(a[b])
}

func Test_r(t *testing.T) {
	fmt.Println(r())
}

func r() int {
	return 1
}

func testKubeAPI() error {
	dataDir := "/var/lib/k8e/k8e"
	certFile := filepath.Join(dataDir, "tls", "client-admin.crt")
	keyFile := filepath.Join(dataDir, "tls", "client-admin.key")
	fmt.Println(certFile, keyFile)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Println(err)
		return err
	}
	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return err
	}
	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return errors.New("failed to parse root certificate")
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            clientCertPool,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		},
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Get("https://127.0.0.1:6443")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
	return nil
}

func Test_Enc(t *testing.T) {
	//text := "helloiloveyou"
	key := "iEuvpsjxsQek7OqMUg/0aQ=="
	//akey := []byte(key)
	// b, err := CBCEncrypt([]byte(text), akey[0:16])
	// if err != nil {
	// 	t.Error(err)
	// }
	// t.Log(Base64EncodeByte(b))
	//b1, _ := Base64DecodeToByte("abnP2ZgEA/W74G4e01XRjw==")
	b := "NKS1ph6/qzF5AiEbhqfIlS5uhlXegQMF/e5zzvcjT5a9j2gBtQPaNk/xBdtxO45viQEfQwAvzKK6Wi0ONOM4OPtZT+MJsdGRK+CmDzcNHjnrAsln7H8YfAd/bcOtDD/ur7pcMkrlCHcO4IolOQe3kMr58kjpwZu/SgB3LZFi+wSBFp3CNdjxPD9CKYCTpCjcUsPwIShHLsGiX4LTKbicr/3BlnX/GLzXt0dPU5QGpYNGrZc6XkAi6Lmv6ZV6i9S2XJBApuJTbWjtRknfY0DbFPZyqdWlpex4egr/NBrVY+In+s8La5mHoNKa7NrIrFQqtoN/o2kB5ivPW6maq7COkNqbBjDujb6DdX0Jr4SP4f6yCmZ7SfqVCRi/MzLO73t0rP47Czyz2yinlSWBa2nr8H8vgHVqn9WWRyx8FiI+dliZfKzjANLyso+l1NxEjWaa7fJGIkMKPx60jDU5RtLwPds7cZSPGNaPI2PDe0YC4VYxyEPpAF2y52BgNig0fwczyvh9LEuAnZoa2jhnq5flWzPYJFTFQK12ET+i0t7P9NiX0ftaiAHpndlxnwqJn632wcCG6vhHAPN+JVkjbKMSUovIH5Oc9m+b3/mxUPyCa892BNwvesymlbNHSqeUGqpnpXJjkMH+rz/cwlD7SVCAVWKO7WHVLJnDoNLsCFSFpBI7fhCcr60s2bG1th3kmIOGMchD6QBdsudgYDYoNH8HM8r4fSxLgJ2aGto4Z6uX5Vsz2CRUxUCtdhE/otLez/TYl9H7WogB6Z3ZcZ8KiZ+t9qeNw8M7lCaLQiPuOM8vwB2ZxyaSV/AJxVGoQZIYAjQ0W5Zbe6MLigP8XFHPwF2vKxGzpYvpVKkdACtRqgjPEfppWig2v3noz2zBOpxK1Dtv5qfD5Uic8Hu1+KH9bHIn5HV+ASLZ7sKXhcO1tyUDU0EjHFJXmceD7Ob80jpR+ar7vcweKN6LQk5I9M0GnorABdU8kttlEaZg9HpBJKBcAz4TtVi1ncgFEOHdw4GEViX3NJS1yX8JzKwpYSusfO4/ja+Fzb1PBwD6jXjpBZoWWJ4ECudSHlRDtuU+PXisSw3JPuAFocsJfXj4jiZRBWXMkJI0PjFEdrLhQ+hLxSsxfJkz4Z6T6qjchyusZ/1MWErzsrtmFgTZ9b83X3dYArjtwA=="
	b1, _ := Base64DecodeToByte(b)
	//akey := []byte(key)
	akey, _ := Base64DecodeToByte(key)
	test1, err := CBCDecrypt([]byte(b1), akey)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(string(test1))
	//if text == string(test1) {
	t.Log("ok 1", string(test1))
	//} else {
	//t.Error("fail", string(test1))
	//}
}

func Test_Enc2(t *testing.T) {

	key := "iEuvpsjxsQek7OqMUg/0aQ=="
	akey, _ := Base64DecodeToByte(key)
	b := "NKS1ph6/qzF5AiEbhqfIlS5uhlXegQMF/e5zzvcjT5a9j2gBtQPaNk/xBdtxO45viQEfQwAvzKK6Wi0ONOM4OPtZT+MJsdGRK+CmDzcNHjnrAsln7H8YfAd/bcOtDD/ur7pcMkrlCHcO4IolOQe3kMr58kjpwZu/SgB3LZFi+wSBFp3CNdjxPD9CKYCTpCjcUsPwIShHLsGiX4LTKbicr/3BlnX/GLzXt0dPU5QGpYNGrZc6XkAi6Lmv6ZV6i9S2XJBApuJTbWjtRknfY0DbFPZyqdWlpex4egr/NBrVY+In+s8La5mHoNKa7NrIrFQqtoN/o2kB5ivPW6maq7COkNqbBjDujb6DdX0Jr4SP4f6yCmZ7SfqVCRi/MzLO73t0rP47Czyz2yinlSWBa2nr8H8vgHVqn9WWRyx8FiI+dliZfKzjANLyso+l1NxEjWaa7fJGIkMKPx60jDU5RtLwPds7cZSPGNaPI2PDe0YC4VYxyEPpAF2y52BgNig0fwczyvh9LEuAnZoa2jhnq5flWzPYJFTFQK12ET+i0t7P9NiX0ftaiAHpndlxnwqJn632wcCG6vhHAPN+JVkjbKMSUovIH5Oc9m+b3/mxUPyCa892BNwvesymlbNHSqeUGqpnpXJjkMH+rz/cwlD7SVCAVWKO7WHVLJnDoNLsCFSFpBI7fhCcr60s2bG1th3kmIOGMchD6QBdsudgYDYoNH8HM8r4fSxLgJ2aGto4Z6uX5Vsz2CRUxUCtdhE/otLez/TYl9H7WogB6Z3ZcZ8KiZ+t9qeNw8M7lCaLQiPuOM8vwB2ZxyaSV/AJxVGoQZIYAjQ0W5Zbe6MLigP8XFHPwF2vKxGzpYvpVKkdACtRqgjPEfppWig2v3noz2zBOpxK1Dtv5qfD5Uic8Hu1+KH9bHIn5HV+ASLZ7sKXhcO1tyUDU0EjHFJXmceD7Ob80jpR+ar7vcweKN6LQk5I9M0GnorABdU8kttlEaZg9HpBJKBcAz4TtVi1ncgFEOHdw4GEViX3NJS1yX8JzKwpYSusfO4/ja+Fzb1PBwD6jXjpBZoWWJ4ECudSHlRDtuU+PXisSw3JPuAFocsJfXj4jiZRBWXMkJI0PjFEdrLhQ+hLxSsxfJkz4Z6T6qjchyusZ/1MWErzsrtmFgTZ9b83X3dYArjtwA=="

	b1, _ := Base64DecodeToByte(b)

	deSrc := string(AesDecrypt(b1, akey))
	fmt.Println("src-->", deSrc)

}

func Test_RSA(t *testing.T) {

	dataStr := "NKS1ph6/qzF5AiEbhqfIlS5uhlXegQMF/e5zzvcjT5a9j2gBtQPaNk/xBdtxO45viQEfQwAvzKK6Wi0ONOM4OPtZT+MJsdGRK+CmDzcNHjnrAsln7H8YfAd/bcOtDD/ur7pcMkrlCHcO4IolOQe3kMr58kjpwZu/SgB3LZFi+wSBFp3CNdjxPD9CKYCTpCjcUsPwIShHLsGiX4LTKbicr/3BlnX/GLzXt0dPU5QGpYNGrZc6XkAi6Lmv6ZV6i9S2XJBApuJTbWjtRknfY0DbFPZyqdWlpex4egr/NBrVY+In+s8La5mHoNKa7NrIrFQqtoN/o2kB5ivPW6maq7COkNqbBjDujb6DdX0Jr4SP4f6yCmZ7SfqVCRi/MzLO73t0rP47Czyz2yinlSWBa2nr8H8vgHVqn9WWRyx8FiI+dliZfKzjANLyso+l1NxEjWaa7fJGIkMKPx60jDU5RtLwPds7cZSPGNaPI2PDe0YC4VYxyEPpAF2y52BgNig0fwczyvh9LEuAnZoa2jhnq5flWzPYJFTFQK12ET+i0t7P9NiX0ftaiAHpndlxnwqJn632wcCG6vhHAPN+JVkjbKMSUovIH5Oc9m+b3/mxUPyCa892BNwvesymlbNHSqeUGqpnpXJjkMH+rz/cwlD7SVCAVWKO7WHVLJnDoNLsCFSFpBI7fhCcr60s2bG1th3kmIOGMchD6QBdsudgYDYoNH8HM8r4fSxLgJ2aGto4Z6uX5Vsz2CRUxUCtdhE/otLez/TYl9H7WogB6Z3ZcZ8KiZ+t9qeNw8M7lCaLQiPuOM8vwB2ZxyaSV/AJxVGoQZIYAjQ0W5Zbe6MLigP8XFHPwF2vKxGzpYvpVKkdACtRqgjPEfppWig2v3noz2zBOpxK1Dtv5qfD5Uic8Hu1+KH9bHIn5HV+ASLZ7sKXhcO1tyUDU0EjHFJXmceD7Ob80jpR+ar7vcweKN6LQk5I9M0GnorABdU8kttlEaZg9HpBJKBcAz4TtVi1ncgFEOHdw4GEViX3NJS1yX8JzKwpYSusfO4/ja+Fzb1PBwD6jXjpBZoWWJ4ECudSHlRDtuU+PXisSw3JPuAFocsJfXj4jiZRBWXMkJI0PjFEdrLhQ+hLxSsxfJkz4Z6T6qjchyusZ/1MWErzsrtmFgTZ9b83X3dYArjtwA=="
	productId := "2c9223b076f6317c0176fee4e10c5e5b"
	prooductVesion := "2.0"
	sign := "arg2z1YysH9XMMs0VAUwHF2WTucPdo9Omp1T5SygtHD8bQwIGUIn4ppHQo4vjxjVqZoydHFvO+T5FoJjD8dYoUb17y65F4yv45P34ttKeUiXpswG39VCwt2pYberU4sNXdDEDa10xCRNdtl2VY7gehIz5VHWGcDa9qOUJH9CqYY="
	publicKey := "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC1Vvxzau7IMu8vkCKHB0VgDZpNdvoUzhZ06B3NC2xbM/erhITPsfOPrJGyFA5Idtu0jMG8As8YtTSj5qtg8RI9F2kMNVsEisxJc6dsWn4dkK2j8pJ+1C2G/grz9PIl1ftVH0vlPYG8C31IdcVgIVJoap6cThek7XF7jpA/NNqOEQIDAQAB"
	err := verifyData(dataStr+productId+prooductVesion, sign, publicKey)
	if err == nil {
		fmt.Println("verify success", sign)
	} else {
		fmt.Println("verify fail", sign)
	}
	sign = "aGVsbG8gbHVvbGlodWk="
	err = verifyData(dataStr+productId+prooductVesion, sign, publicKey)
	if err == nil {
		fmt.Println("verify success", sign)
	} else {
		fmt.Println("verify fail", sign)
	}
}

func Test_ar(t *testing.T) {
	sql := `100 - ((node_filesystem_avail_bytes{device=~"%s"} * 100) / node_filesystem_size_bytes{device=~"%s"})`
	promQL := fmt.Sprintf(sql, "/dev/vdb", "/dev/vdb", "/dev/vdb", "/dev/vdb")
	fmt.Println(promQL)
}

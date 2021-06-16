package test

import (
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	publicKey  = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCeRsyfEMUo6PFs7upFQJr15CUveSDVvWAg8KRXz1fU5XwBgc6d2YY7ly3QeuKz7_rd5nlXlUxnMzT2dVhTPfNEI1Q4AnNbFxqhNN3gEeTUlIh1lBXdfoWOsOOb3LHs0124BgTViTRjDW-1CmJk-vqGq-maOaA7BcXRlt8kMecJdwIDAQAB"
	privateKey = "MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAJ5GzJ8QxSjo8Wzu6kVAmvXkJS95INW9YCDwpFfPV9TlfAGBzp3ZhjuXLdB64rPv-t3meVeVTGczNPZ1WFM980QjVDgCc1sXGqE03eAR5NSUiHWUFd1-hY6w45vcsezTXbgGBNWJNGMNb7UKYmT6-oar6Zo5oDsFxdGW3yQx5wl3AgMBAAECgYBedfCfKjoQ3V1g3wHQDOuuvtd2irsO9TPO1O-wPF22ALPOjnMKgAz9uY8tMnnkW-AD2Q4oOEFeAhCk2om5PGrXGThggJNKRrqYeeyKDtGdtdnp8Za9nI1jeVz-LeNNQHNPhIuhR5z8ufDaWVCm7sNcGyIbP_qg2SErg3yJpypcIQJBAN3trJ_z-McAmV6o_xJm5mk094tols74Ix59F60i0mB4VWdFY3M9fHjIvCKKavkGb4szl4FAiklsKO5myb64eCcCQQC2k3PV7dPiTs1_HDyP6ZuFTfuPT_r7llj8PSQXQPXCJb5o-oKXTsex06C8J6R0EpXCiBZhmWFe-Hqb1nIAU-YxAkBsQp8tQDShz1cB6GrVrUDFHcOMTC8VM9Ld8qP0H8KEsO7oe97xvpLT0QiFyQQ6CrurKjXEJZnQC2VENvw_f3mNAkAbkwWJp9O6eEBdFDypV5TfezmlGWVEnh5uaiWLRYpYei7Z2AvlIkbSuq2p_Sq_RRdNPBR1RR8JoumRo7-wAPvhAkBCm_ylQbGV8McWqzjnlFKd5Fz5SDrTYAC-xJcQ433jiFxAVseWKTpOYb7XR4AhDXSk7GFLseQMv-IdnR5fh7yx"
)

func aa() {

}

func signData(m map[string]string) error {

	m["timestamp"] = fmt.Sprintf("%d", time.Now().UnixNano()/1e6)

	sig := url.Values{}
	for key, value := range m {
		sig.Add(key, value)
	}

	//进行转码使之可以安全的用在URL查询里
	quUrl, err := url.QueryUnescape(sig.Encode())
	if err != nil {
		fmt.Println("QueryUnescape", err)
		return err
	}

	if out, err := RsaSignWithMd5(quUrl, privateKey); err != nil {
		return err
	} else {
		m["sig"] = out
		return nil
	}
}

func verifyData(data, sign, publicKey string) error {
	// sign := m["sig"]
	// delete(m, "sig")

	// sig := url.Values{}
	// for key, value := range m {
	// 	sig.Add(key, value)
	// }

	// //进行转码使之可以安全的用在URL查询里
	// quUrl, _ := url.QueryUnescape(sig.Encode())

	return RsaVerifySignWithMd5(data, sign, publicKey)
}

func Base64DecodeToByte(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func Base64EncodeByte(s []byte) string {
	return base64.StdEncoding.EncodeToString(s)
}

// 签名
func RsaSignWithMd5(data string, prvKey string) (sign string, err error) {

	//如果密钥是urlSafeBase64的话需要处理下
	prvKey = Base64URLDecode2(prvKey)

	keyBytes, err := base64.StdEncoding.DecodeString(prvKey)
	if err != nil {
		fmt.Println("DecodeString:", err)
		return "", err
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(keyBytes)
	if err != nil {
		fmt.Println("ParsePKCS8PrivateKey", err)
		return "", err
	}

	h := md5.New()
	h.Write([]byte(data))
	hash := h.Sum(nil)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey.(*rsa.PrivateKey), crypto.MD5, hash[:])
	if err != nil {
		fmt.Println("SignPKCS1v15:", err)
		return "", err
	}

	out := base64.RawURLEncoding.EncodeToString(signature)
	return out, nil
}

// 验签
func RsaVerifySignWithMd5(originalData, signData, pubKey string) error {

	//TODO : 验证时间

	sign, err := Base64DecodeToByte(signData)
	if err != nil {
		fmt.Println("DecodeString:", err)
		return err
	}

	//pubKey = Base64URLDecode2(pubKey)

	public, err := Base64DecodeToByte(pubKey)
	if err != nil {
		fmt.Println("DecodeString")
		return err
	}

	pub, err := x509.ParsePKIXPublicKey(public)
	if err != nil {
		fmt.Println("ParsePKIXPublicKey", err)
		return err
	}

	hash := md5.New()
	hash.Write([]byte(originalData))
	return rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.MD5, hash.Sum(nil), sign)
}

//因为Base64转码后可能包含有+,/,=这些不安全的URL字符串，所以要进行换字符
// '+' -> '-'
// '/' -> '_'
// '=' -> ''
// 字符串长度不足4倍的位补"="

func Base64URLDecode2(data string) string {
	var missing = (4 - len(data)%4) % 4
	data += strings.Repeat("=", missing) //字符串长度不足4倍的位补"="
	data = strings.Replace(data, "_", "/", -1)
	data = strings.Replace(data, "-", "+", -1)
	return data
}

func Base64UrlSafeEncode2(data string) string {
	safeUrl := strings.Replace(data, "/", "_", -1)
	safeUrl = strings.Replace(safeUrl, "+", "-", -1)
	safeUrl = strings.Replace(safeUrl, "=", "", -1)
	return safeUrl
}

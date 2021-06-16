package test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
)

var commonIV = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

/**
 * 解密
 *
 * @param  cipherText 密文
 * @param  keyText    密钥
 *
 * @return plainText  明文
 * @return err        错误
 */
func Decrypt(cipherText, keyText string) (plainText string, err error) {
	// 解密的密文
	ciphertext, _ := hex.DecodeString(cipherText)

	// 创建加密算法aes
	c, errors := aes.NewCipher([]byte(keyText))
	if errors != nil {
		fmt.Printf("Error: NewCipher(%d bytes) = %s", len(keyText), errors)
		return "", errors
	}

	// 解密字符串
	cfbdec := cipher.NewCFBDecrypter(c, commonIV)
	plaintextCopy := make([]byte, len(ciphertext))
	cfbdec.XORKeyStream(plaintextCopy, ciphertext)

	return fmt.Sprintf("%s", plaintextCopy), nil
}

//AES CBC加密
//plainText 明文
//keyText 密钥 固定16位
//iv 偏移量，可选，16位
func CBCEncrypt(plainText, keyText []byte, iv ...[]byte) ([]byte, error) {

	//	if len(keyText) != 16 {
	//		return nil, errors.New("crypto/cipher: keyText not eq 16")
	//	}
	// 加密的明文

	var b_iv []byte
	if len(iv) > 0 {
		if len(iv[0]) != 16 {
			return nil, errors.New("crypto/cipher: iv not eq 16")
		}
		b_iv = iv[0][:16]
	} else {
		b_iv = keyText[:aes.BlockSize]
	}

	// 创建加密算法aes
	key := keyText[:aes.BlockSize]
	c, err := aes.NewCipher(key)
	plainText = PKCS5Padding2(plainText, c.BlockSize())
	if err != nil {
		fmt.Printf("Error: NewCipher(%d bytes) = %s", len(keyText), err)
		return nil, err
	}
	mode := cipher.NewCBCEncrypter(c, b_iv)
	encryptText := make([]byte, len(plainText))
	mode.CryptBlocks(encryptText, plainText)
	return encryptText, nil
}

func PKCS5Padding2(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)

}

func PKCS5UnPadding2(origData []byte) ([]byte, error) {
	length := len(origData)
	// 去掉最后一个字节 unpadding 次
	unpadding := int(origData[length-1])
	cut := length - unpadding

	if cut < 0 || length < cut {
		return nil, errors.New("Can not unpadding data")
	}
	return origData[:cut], nil
}

//AES CBC解密
//cryptedText 密文
//keyText 密钥 固定16位
//iv 偏移量，可选，16位
func CBCDecrypt(cryptedText []byte, keyText []byte, iv ...[]byte) ([]byte, error) {

	//	if len(keyText) != 16 {
	//		return nil, errors.New("crypto/cipher: keyText not eq 16")
	//	}
	plaintext := make([]byte, len(cryptedText))

	// 长度不能小于aes.Blocksize
	if len(cryptedText) < aes.BlockSize {
		return nil, errors.New("crypto/cipher: ciphertext too short")
	}

	var b_iv []byte
	if len(iv) > 0 {
		if len(iv[0]) != 16 {
			return nil, errors.New("crypto/cipher: iv not eq 16")
		}
		b_iv = iv[0][:16]
	} else {
		b_iv = keyText[:aes.BlockSize]
	}
	// 验证输入参数
	// 必须为aes.Blocksize的倍数
	if len(plaintext)%aes.BlockSize != 0 {
		return nil, errors.New("crypto/cipher: ciphertext is not a multiple of the block size")
	}
	block, err := aes.NewCipher(keyText[:aes.BlockSize])
	if err != nil {
		return nil, err
	}

	mode := cipher.NewCBCDecrypter(block, b_iv)
	mode.CryptBlocks(plaintext, cryptedText)
	plaintext, err = PKCS5UnPadding2(plaintext)
	return plaintext, err
}

func MD5String(str string, salt ...string) string {
	if str == "" {
		return ""
	}
	h := md5.New()
	h.Write([]byte(str))
	if len(salt) > 0 {
		bs := h.Sum([]byte(salt[0]))
		return fmt.Sprintf("%x", bs)
	} else {
		bs := h.Sum(nil)
		return fmt.Sprintf("%x", bs)
	}

}

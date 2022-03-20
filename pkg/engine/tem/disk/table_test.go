package disk

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func Test_EncodeDecodeValueIndex(t *testing.T) {
	var dst [3]byte
	binary.PutUvarint(dst[0:], 65535)
	fmt.Println(dst)
}

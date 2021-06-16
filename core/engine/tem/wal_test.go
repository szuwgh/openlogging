package tem

import (
	"fmt"
	"os"
	"testing"

	"github.com/sophon-lab/temsearch/core/engine/tem/byteutil"
)

var testMsg = `[
    {
        "message": "a", 
        "tags": {
            "job":"wo1",
            "instance":"172.18.5.20"
        }
    },
    { 
        "message": "a c b",
        "tags":  {
            "job":"wo2",
            "instance":"172.18.5.22"
        }
    },
    { 
        "message": "a c b f",
        "tags":  {
            "job":"wo3",
            "instance":"172.18.5.23"
        }
    }
]`

func Test_Wal(t *testing.T) {
	// f, err := os.OpenFile("./wal.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	walw, _ := newWalWriter("./wal.log")

	b := []byte(testMsg)
	walw.log(b)
	walw.log(b)
	walw.log(b)
	walw.log(b)

	//walw.flush()
	//walw.close()
}

func Test_WalReader(t *testing.T) {
	fname := "E:\\goproject\\temsearch2\\src\\data\\wal\\000001"
	//fname := "./wal.log"
	f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	walr := newWalReaderForIO(f)
	buf := &byteutil.Buffer{}
	for walr.next() {
		buf.Reset()
		n, err := buf.ReadFrom(walr)
		if err != nil || n == 0 {
			fmt.Println(err, n)
			break
		}
		fmt.Println(n, string(buf.Bytes()))
	}
	f.Close()
}

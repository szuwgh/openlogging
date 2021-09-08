package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var testMsg = `[
    {
        "message": "a a a", 
        "tags": {
            "job":"kingdee",
            "instance":"172.18.5.20"
        }
    },
    { 
        "message": "a b c",
        "tags":  {
            "job":"paas",
            "instance":"172.18.5.22"
        }
    },
    { 
        "message": "c e f",
        "tags":  {
            "job":"IDC",
            "instance":"172.18.5.23"
        }
    }
]`

func main() {
	client := &http.Client{}
	//生成要访问的url
	url := "http://localhost:9400/index"

	t := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-t.C:
			//提交请求
			r := strings.NewReader(testMsg)
			reqest, err := http.NewRequest("POST", url, r)
			if err != nil {
				fmt.Println(err)
				continue
			}
			//处理返回结果
			response, err := client.Do(reqest)
			if err != nil {
				fmt.Println(err)
				continue
			}
			//defer response.Body.Close()
			b, _ := ioutil.ReadAll(response.Body)
			fmt.Println(time.Now().Local().Format("2006-01-02 15:04:05"), string(b))
		}

	}
}

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
        "message": "There were a sensitivity and a beauty to her that have nothing to do with looks. She was one to be listened to, whose words were so easy to take to heart", 
        "tags": {
            "job":"kingdee",
            "instance":"172.18.5.20"
        }
    },
    { 
        "message": "In some cases it can even be fatal, if pleasure is one's truth and its attainment more important than life itself. In other lives, though, the search for what is truthful gives life",
        "tags":  {
            "job":"paas",
            "instance":"172.18.5.22"
        }
    },
    { 
        "message": "The notes fascinated me. Here was someone immersed in a search for truth and beauty. Words had been treasured, words that were beautiful. And I felt as if the words somehow delighted in being discovered",
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

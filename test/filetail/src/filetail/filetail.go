package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

var sentence = [6]string{"aa bb cc dd ee", "aa1 bb1 cc1 dd1 ee1", "aa2 bb2 cc2 dd2 ee2", "aa3 bb3 cc3 dd3 ee3", "aa4 bb4 cc4 dd4 ee4", "aa5 bb5 cc5 dd5 ee5"}

func main() {
	file := "./log.txt"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if nil != err {
		panic(err)
	}
	loger := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile) //
	//var sentence []string
	var i int
	go func() {
		for {
			// for i := 0; i < 5; i++ {
			// 	sentence = append(sentence, s[0])
			// }
			loger.Println(sentence[i])
			//sentence = sentence[:0]
			time.Sleep(5 * time.Second)
			i++
			if i > 5 {
				i = 0
			}
		}
	}()
	fmt.Println("start file log with 5 second")
	select {}
}

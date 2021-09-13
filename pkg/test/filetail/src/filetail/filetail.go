package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ser163/WordBot/generate"
)

func main() {
	file := "E:\\promtail\\log.txt"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if nil != err {
		panic(err)
	}
	loger := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)
	var sentence []string
	go func() {
		for {
			for i := 0; i < 5; i++ {
				w, err := generate.GenRandomWorld(0, "none")
				if err != nil {
					fmt.Println(err)
					break
				}
				sentence = append(sentence, w.Word)
			}
			loger.Println(time.Now().Format("2006-01-02: 15:04:05"), sentence)
			sentence = sentence[:0]
			time.Sleep(5 * time.Second)
		}
	}()
	select {}
}

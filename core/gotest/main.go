package main

import (
	"fmt"
	"runtime"
	"time"
)

// func main() {
// 	runtime.GOMAXPROCS(1)
// 	go func() {
// 		for {
// 		}
// 	}()
// 	go func() {
// 		for i := 0; i < 6; i++ {
// 			fmt.Print(i)
// 		}
// 	}()
// 	fmt.Print(1)
// 	time.Sleep(10 * time.Second)
// }

func main() {
	runtime.GOMAXPROCS(1)
	A := []int{1, 2, 3, 4, 5, 6}
	B := []string{"a", "b", "c", "d", "e", "f"}
	go func() {
		for i := 0; i < 6; i++ {
			fmt.Print(A[i])
			runtime.Gosched()
		}
	}()
	go func() {
		for i := 0; i < 6; i++ {
			fmt.Print(B[i])
			runtime.Gosched()
		}
	}()
	time.Sleep(10 * time.Second)
}

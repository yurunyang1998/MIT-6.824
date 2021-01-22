package main

import (
	"fmt"
	"math/rand"
	"time"
)

type test struct {
	channel chan int
}

func main() {
	now := time.Now().UnixNano()
	for i := 0; i < 10032; i++ {
		// fmt.Print("H")
	}
	lastBeatHeartTime := time.Now().UnixNano()
	elaspe := (lastBeatHeartTime - now) / int64(time.Millisecond)
	fmt.Println(elaspe, int64(time.Millisecond))
	fmt.Printf("%T", now)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 10; i++ {
		fmt.Println(r.Intn(150) + 150)
	}

}

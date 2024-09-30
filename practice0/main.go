package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	b := [100]byte{}
	for {
		go func() {
			filler(b[:50], '0', '1')
			time.Sleep(1 * time.Second)
		}()
		go func() {
			filler(b[50:], 'X', 'Y')
			time.Sleep(1 * time.Second)
		}()
		go func() {
			go fmt.Println(string(b[:]))
			time.Sleep(1 * time.Second)
		}()
	}
}

func filler(b []byte, ifzero byte, ifnot byte) {
	for i := range b {
		if rand.Intn(2) == 0 {
			b[i] = ifzero
		} else {
			b[i] = ifnot
		}
	}
}

package main

import (
	"fmt"
	"time"
)

func main() {
	records := make(chan struct{}, 10)
	timeout := time.NewTicker(time.Second * 5)
	// timeout := time.After(time.Second * 3)
	go func() {
		for {
			select {
			case r := <-records:
				fmt.Println("r", r)
				timeout.Reset(time.Second * 5)
			case <-timeout.C:
				fmt.Println("time out")
			}
		}
	}()
	for i := 0; ; i++ {
		if i >= 100 {
			break
		}
		records <- struct{}{}
	}
	for {

		fmt.Println("over")
		time.Sleep(time.Second * 10)
	}
}

package main

import (
	"fmt"
	"strconv"
)

func main() {
	records := make([]string, 0)

	for i := 0; i < 100; i++ {
		records = append(records, strconv.Itoa(i))
	}
	for i := len(records) - 1; i >= 0; i-- {
		if i%2 == 0 {
			records = append(records[:i], records[i+1:]...)
		}
	}
	fmt.Println(records)
}

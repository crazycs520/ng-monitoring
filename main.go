package main

import (
	"log"
	"time"

	"github.com/google/gops/agent"
)

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	total := 1 * 1024 * 1024 * 1024
	num := 20
	single := total / num
	cache := make([][]byte, num)
	cnt := 0
	for {
		cnt++
		m := make([]byte, single)
		for i := range m {
			m[i] = byte(i % 255)
		}
		idx := cnt % num
		cache[idx] = m
		time.Sleep(time.Millisecond * 50)
	}
}

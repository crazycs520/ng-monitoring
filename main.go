package main

import (
	"log"
	"math/rand"
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
		size := rand.Intn(single * 2)
		m := make([]byte, single+size)
		for i := range m {
			m[i] = byte(i % 255)
		}
		idx := cnt % num
		cache[idx] = m

		for i := range m {
			m[i] = byte(i % 255)
		}
	}
}

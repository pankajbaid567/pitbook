// Command noisefloor is the control for the latency benchmarks: it measures how often
// the machine stalls a plain spinning thread, with no engine involved. Tail latencies
// in BENCHMARKS.md cannot be better than this floor suggests.
package main

import (
	"fmt"
	"time"
)

func main() {
	for round := 0; round < 3; round++ {
		end := time.Now().Add(10 * time.Second)
		last := time.Now()
		var n1, n5, n10, n50 int
		var max time.Duration
		for last.Before(end) {
			now := time.Now()
			g := now.Sub(last)
			last = now
			if g > time.Millisecond {
				n1++
			}
			if g > 5*time.Millisecond {
				n5++
			}
			if g > 10*time.Millisecond {
				n10++
			}
			if g > 50*time.Millisecond {
				n50++
			}
			if g > max {
				max = g
			}
		}
		fmt.Printf("10s spin loop, nothing else running: stalls >1ms=%d >5ms=%d >10ms=%d >50ms=%d max=%s\n", n1, n5, n10, n50, max)
	}
}

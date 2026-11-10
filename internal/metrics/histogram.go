// Package metrics holds the latency histogram used by benchmarks and /metrics.
package metrics

import (
	"math/bits"
	"time"
)

// Histogram records latencies in nanoseconds with ~3% relative resolution
// (32 linear sub-buckets per power of two). Adapted from the microsecond
// histogram in cache-cluster; matching latency is sub-microsecond, so the unit
// here is nanoseconds. Not safe for concurrent use: give each worker its own
// and Merge afterwards.
type Histogram struct {
	counts [64 + 40*32]int64
	n      int64
	sum    int64
	max    int64
}

func bucket(v int64) int {
	if v < 64 {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 6
	m := v >> e
	return 64 + (e-1)*32 + int(m-32)
}

func lowerBound(i int) int64 {
	if i < 64 {
		return int64(i)
	}
	j := i - 64
	e := j/32 + 1
	m := int64(j%32 + 32)
	return m << e
}

// Record adds one observation.
func (h *Histogram) Record(d time.Duration) {
	ns := int64(d)
	if ns < 0 {
		ns = 0
	}
	i := bucket(ns)
	if i >= len(h.counts) {
		i = len(h.counts) - 1
	}
	h.counts[i]++
	h.n++
	h.sum += ns
	if ns > h.max {
		h.max = ns
	}
}

// Merge adds o's observations to h.
func (h *Histogram) Merge(o *Histogram) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.n += o.n
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() int64 { return h.n }

// MeanNS returns the mean in nanoseconds.
func (h *Histogram) MeanNS() float64 {
	if h.n == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.n)
}

// MaxNS returns the largest observation in nanoseconds.
func (h *Histogram) MaxNS() int64 { return h.max }

// PercentileNS returns the q-th percentile (0<q<=100) in nanoseconds (the
// lower bound of the bucket that holds it).
func (h *Histogram) PercentileNS(q float64) int64 {
	if h.n == 0 {
		return 0
	}
	target := int64(float64(h.n)*q/100 + 0.999999)
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			return lowerBound(i)
		}
	}
	return h.max
}

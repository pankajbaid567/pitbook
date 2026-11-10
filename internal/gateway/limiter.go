package gateway

import (
	"sync"
	"time"
)

// bucket is a token bucket: rate tokens per second up to burst.
type bucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
}

func newBucket(rate, burst float64) *bucket {
	return &bucket{tokens: burst, last: time.Now(), rate: rate, burst: burst}
}

func (b *bucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// ipLimiter keeps one bucket per client address and forgets idle ones.
type ipLimiter struct {
	mu      sync.Mutex
	m       map[string]*bucket
	rate    float64
	burst   float64
	lastGC  time.Time
	maxKeys int
}

func newIPLimiter(rate, burst float64) *ipLimiter {
	return &ipLimiter{m: map[string]*bucket{}, rate: rate, burst: burst, lastGC: time.Now(), maxKeys: 10000}
}

func (l *ipLimiter) allow(key string) bool {
	l.mu.Lock()
	b, ok := l.m[key]
	if !ok {
		if len(l.m) >= l.maxKeys || time.Since(l.lastGC) > time.Minute {
			// Drop buckets that have refilled (idle); bounds memory under address churn.
			for k, v := range l.m {
				v.mu.Lock()
				idle := time.Since(v.last) > 30*time.Second
				v.mu.Unlock()
				if idle || len(l.m) >= l.maxKeys {
					delete(l.m, k)
				}
			}
			l.lastGC = time.Now()
		}
		b = newBucket(l.rate, l.burst)
		l.m[key] = b
	}
	l.mu.Unlock()
	return b.allow()
}

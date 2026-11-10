package metrics

import (
	"testing"
	"time"
)

func TestPercentilesWithinResolution(t *testing.T) {
	var h Histogram
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	if h.Count() != 1000 {
		t.Fatalf("count %d", h.Count())
	}
	p50 := float64(h.PercentileNS(50))
	if p50 < 500_000*0.95 || p50 > 500_000*1.05 {
		t.Fatalf("p50 %v outside 5%% of 500us", p50)
	}
	if h.MaxNS() != 1_000_000 {
		t.Fatalf("max %d", h.MaxNS())
	}
}

func TestMerge(t *testing.T) {
	var a, b Histogram
	a.Record(time.Microsecond)
	b.Record(2 * time.Microsecond)
	a.Merge(&b)
	if a.Count() != 2 || a.MaxNS() != 2000 {
		t.Fatalf("merge: count %d max %d", a.Count(), a.MaxNS())
	}
}

func TestEmpty(t *testing.T) {
	var h Histogram
	if h.PercentileNS(99) != 0 || h.MeanNS() != 0 {
		t.Fatal("empty histogram must report zeros")
	}
}

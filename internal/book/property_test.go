package book

import (
	"math/rand/v2"
	"os"
	"testing"
)

// genCmd produces a random command. Participants, prices and quantities are
// drawn from small ranges so that crossing, self-trades, FOK failures, stale
// cancels and priority changes all happen often.
func genCmd(rng *rand.Rand, issued []OrderID, coid uint64) Command {
	part := uint32(rng.IntN(4) + 1)
	roll := rng.IntN(100)
	switch {
	case roll < 72:
		c := Command{Kind: CmdNew, Participant: part, ClientOID: coid, Side: Side(rng.IntN(2) + 1),
			Price: Price(95 + rng.IntN(11)), Qty: Qty(rng.IntN(12) + 1)}
		switch r := rng.IntN(100); {
		case r < 68:
			c.Type = Limit
		case r < 78:
			c.Type = Market
			c.Price = 0
		case r < 90:
			c.Type = IOC
		default:
			c.Type = FOK
		}
		if rng.IntN(200) == 0 { // occasional invalid input
			c.Qty = Qty(rng.IntN(3) - 1)
		}
		return c
	case roll < 88:
		return Command{Kind: CmdCancel, Participant: part, ClientOID: coid, Target: pick(rng, issued)}
	default:
		return Command{Kind: CmdReplace, Participant: part, ClientOID: coid, Target: pick(rng, issued),
			Price: Price(95 + rng.IntN(11)), Qty: Qty(rng.IntN(12) + 1)}
	}
}

func pick(rng *rand.Rand, issued []OrderID) OrderID {
	if len(issued) == 0 || rng.IntN(20) == 0 {
		return OrderID(rng.IntN(1000) + 1) // possibly unknown
	}
	n := len(issued)
	lo := 0
	if n > 60 {
		lo = n - 60 // bias toward recent orders, which are more likely to still rest
	}
	return issued[lo+rng.IntN(n-lo)]
}

func sameEvents(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPropertyEngineEqualsReference is the correctness proof for the book: for
// random command streams the real book and the naive reference must produce
// identical event streams, and the real book must satisfy its invariants after
// every single command.
func TestPropertyEngineEqualsReference(t *testing.T) {
	seeds, perSeed := 5, 40_000 // 200k commands, a few seconds
	if os.Getenv("PITBOOK_PROP_FULL") == "1" {
		perSeed = 200_000 // 1,000,000 commands, about 90 s (make prop)
	}
	if testing.Short() {
		seeds, perSeed = 2, 20_000
	}
	var total, fillsSeen, stpSeen, fokRej, resting int
	for s := 0; s < seeds; s++ {
		rng := rand.New(rand.NewPCG(uint64(s)+1, 0x9e3779b97f4a7c15))
		b := New()
		rf := &ref{}
		var issued []OrderID
		var buf []Event
		for i := 1; i <= perSeed; i++ {
			seq := uint64(i)
			c := genCmd(rng, issued, seq)
			buf = b.Apply(seq, c, buf[:0])
			want := rf.apply(seq, c)
			if !sameEvents(buf, want) {
				t.Fatalf("seed %d seq %d cmd %+v\n real: %s\n want: %s", s, seq, c, fmtEvents(buf), fmtEvents(want))
			}
			if err := b.Check(); err != nil {
				t.Fatalf("seed %d seq %d: %v", s, seq, err)
			}
			if b.Len() != len(rf.resting) {
				t.Fatalf("seed %d seq %d: resting %d vs %d", s, seq, b.Len(), len(rf.resting))
			}
			checkConservation(t, c, buf)
			for _, e := range buf {
				switch {
				case e.Kind == EvAck:
					issued = append(issued, e.OrderID)
				case e.Kind == EvFill:
					fillsSeen++
				case e.Kind == EvCanceled && e.Code == ReasonSelfTrade:
					stpSeen++
				case e.Kind == EvReject && e.Code == RejFOKNotFillable:
					fokRej++
				}
			}
			total++
			resting += b.Len()
		}
	}
	t.Logf("commands=%d fills=%d self-trade-cancels=%d fok-rejects=%d avg-resting=%d", total, fillsSeen, stpSeen, fokRej, resting/total)
	if fillsSeen == 0 || stpSeen == 0 || fokRej == 0 {
		t.Fatalf("generator did not exercise the rules: fills=%d stp=%d fok=%d", fillsSeen, stpSeen, fokRej)
	}
}

// checkConservation verifies per-command arithmetic that the reference cannot
// catch if both are wrong in the same way.
func checkConservation(t *testing.T, c Command, evs []Event) {
	t.Helper()
	var filled Qty
	for _, e := range evs {
		switch e.Kind {
		case EvFill:
			if e.Participant == e.Counter {
				t.Fatalf("self-trade fill: %s", fmtEvent(e))
			}
			if e.Qty <= 0 || e.Price <= 0 {
				t.Fatalf("bad fill %s", fmtEvent(e))
			}
			if c.Kind == CmdNew && c.Type != Market {
				if (c.Side == Buy && e.Price > c.Price) || (c.Side == Sell && e.Price < c.Price) {
					t.Fatalf("fill outside limit: %+v %s", c, fmtEvent(e))
				}
			}
			filled += e.Qty
		}
	}
	if c.Kind == CmdNew && c.Qty > 0 && filled > c.Qty {
		t.Fatalf("overfill: filled %d of %d", filled, c.Qty)
	}
	if c.Kind == CmdNew && c.Type == FOK {
		_, rej := find(evs, EvReject)
		if !rej && filled != c.Qty {
			t.Fatalf("FOK accepted but filled %d of %d", filled, c.Qty)
		}
		if rej && filled != 0 {
			t.Fatal("FOK rejected with fills")
		}
	}
}

// TestDeterministicReplay: the same command stream always yields the same
// events and the same state hash, regardless of Go's map iteration order.
func TestDeterministicReplay(t *testing.T) {
	gen := func() (uint64, uint64) {
		rng := rand.New(rand.NewPCG(42, 7))
		b := New()
		var issued []OrderID
		h := newHasher()
		var buf []Event
		for i := 1; i <= 30_000; i++ {
			c := genCmd(rng, issued, uint64(i))
			buf = b.Apply(uint64(i), c, buf[:0])
			for _, e := range buf {
				if e.Kind == EvAck {
					issued = append(issued, e.OrderID)
				}
				h.u64(uint64(e.Kind)<<56 | uint64(e.Code)<<48 | uint64(e.Idx))
				h.u64(e.Seq)
				h.u64(uint64(e.Price))
				h.u64(uint64(e.Qty))
				h.u64(e.TradeID)
			}
		}
		return uint64(h), b.Hash()
	}
	e0, s0 := gen()
	for i := 0; i < 20; i++ {
		if e, s := gen(); e != e0 || s != s0 {
			t.Fatalf("run %d diverged: events %x vs %x, state %x vs %x", i, e, e0, s, s0)
		}
	}
}

func BenchmarkSteadyMix(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	bk := New()
	var issued []OrderID
	var buf []Event
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		c := genCmd(rng, issued, uint64(i))
		buf = bk.Apply(uint64(i), c, buf[:0])
		for _, e := range buf {
			if e.Kind == EvAck {
				issued = append(issued, e.OrderID)
			}
		}
		if len(issued) > 4096 {
			issued = issued[2048:]
		}
	}
}

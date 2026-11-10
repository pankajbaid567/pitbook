package bots

import (
	"testing"

	"pitbook/internal/book"
	"pitbook/internal/engine"
)

func runBots(t *testing.T, seed uint64, steps int) (uint64, uint64, *Market) {
	t.Helper()
	e, _, err := engine.Open(engine.Config{Symbol: "T", NoWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := New(Config{Seed: seed, Mid: 10000, ClientOIDBase: 1000, PartID: func(s string) uint32 {
		var h uint32 = 2166136261
		for i := 0; i < len(s); i++ {
			h = (h ^ uint32(s[i])) * 16777619
		}
		return h
	}}, EngineExec(e))
	for i := 0; i < steps; i++ {
		m.Step()
	}
	var h, seq uint64
	var checkErr error
	e.Do(func(mc *engine.Machine) { h, seq = mc.Hash(), mc.LastSeq(); checkErr = mc.Book().Check() })
	if checkErr != nil {
		t.Fatal(checkErr)
	}
	return h, seq, m
}

func TestSameSeedSameMarket(t *testing.T) {
	h1, s1, _ := runBots(t, 7, 4000)
	h2, s2, _ := runBots(t, 7, 4000)
	if h1 != h2 || s1 != s2 {
		t.Fatalf("same seed diverged: %x/%d vs %x/%d", h1, s1, h2, s2)
	}
	h3, _, _ := runBots(t, 8, 4000)
	if h3 == h1 {
		t.Fatal("different seeds produced the same market")
	}
}

func TestMarketIsLively(t *testing.T) {
	_, seq, m := runBots(t, 1, 5000)
	if m.Fills < 300 || seq < 2500 || m.Errors != 0 {
		t.Fatalf("market too quiet or erroring: seq=%d fills=%d errors=%d", seq, m.Fills, m.Errors)
	}
	if m.Mid() < 9000 || m.Mid() > 11000 {
		t.Fatalf("mid wandered off: %d", m.Mid())
	}
}

func TestBookStaysBounded(t *testing.T) {
	e, _, _ := engine.Open(engine.Config{Symbol: "T", NoWAL: true})
	defer e.Close()
	m := New(Config{Seed: 3, MaxQuotes: 10}, EngineExec(e))
	for i := 0; i < 20000; i++ {
		m.Step()
	}
	var n int
	e.Do(func(mc *engine.Machine) { n = mc.Book().Len() })
	if n > 3*10+4*3 { // 3 makers x 10 quotes + 4 takers x 3 resting limits
		t.Fatalf("book grew to %d orders", n)
	}
	_ = book.Buy
}

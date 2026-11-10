package book

import "testing"

func TestLimitRestsAndDelta(t *testing.T) {
	r := newRunner(t)
	ev := r.do(newCmd(1, 1, Buy, Limit, 100, 10))
	if len(ev) != 2 || ev[0].Kind != EvAck || ev[0].OrderID != 1 || ev[1].Kind != EvDelta || ev[1].Qty != 10 || ev[1].Side != Buy {
		t.Fatalf("unexpected events: %s", fmtEvents(ev))
	}
	if p, ok := r.b.Best(Buy); !ok || p != 100 {
		t.Fatalf("best bid = %d,%v", p, ok)
	}
}

func TestPricePriorityThenTime(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 101, 5)) // id 1
	r.do(newCmd(2, 1, Sell, Limit, 100, 5)) // id 2: better price
	r.do(newCmd(3, 1, Sell, Limit, 100, 5)) // id 3: same price, later
	ev := r.do(newCmd(4, 1, Buy, Limit, 101, 12))
	f := fills(ev)
	if len(f) != 3 {
		t.Fatalf("want 3 fills, got %s", fmtEvents(ev))
	}
	// 5 from id2 @100, 5 from id3 @100, then 2 from id1 @101 (resting price, not taker limit).
	want := []struct {
		maker OrderID
		price Price
		qty   Qty
	}{{2, 100, 5}, {3, 100, 5}, {1, 101, 2}}
	for i, w := range want {
		if f[i].MakerID != w.maker || f[i].Price != w.price || f[i].Qty != w.qty || f[i].TakerID != 4 {
			t.Fatalf("fill %d = %s", i, fmtEvent(f[i]))
		}
	}
	if o, ok := r.b.Order(1); !ok || o.Rem != 3 {
		t.Fatalf("order 1 should have 3 left")
	}
}

func TestTradeAtRestingPriceNotLimit(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 5))
	ev := r.do(newCmd(2, 1, Buy, Limit, 110, 5))
	if f := fills(ev); len(f) != 1 || f[0].Price != 100 {
		t.Fatalf("trade must be at maker price 100: %s", fmtEvents(ev))
	}
	if r.b.Len() != 0 {
		t.Fatal("book should be empty")
	}
}

func TestMarketOrderNoLiquidityRemainder(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 4))
	ev := r.do(newCmd(2, 1, Buy, Market, 0, 10))
	c, ok := find(ev, EvCanceled)
	if !ok || c.Code != ReasonNoLiquidity || c.Qty != 6 {
		t.Fatalf("want NO_LIQUIDITY cancel of 6, got %s", fmtEvents(ev))
	}
	if r.b.Len() != 0 {
		t.Fatal("market remainder must not rest")
	}
}

func TestIOCRemainderCancelled(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 4))
	ev := r.do(newCmd(2, 1, Buy, IOC, 100, 10))
	c, ok := find(ev, EvCanceled)
	if !ok || c.Code != ReasonIOC || c.Qty != 6 {
		t.Fatalf("want IOC_REMAINDER of 6, got %s", fmtEvents(ev))
	}
	if _, ok := r.b.Best(Buy); ok {
		t.Fatal("IOC must not rest")
	}
}

func TestFOKAllOrNothing(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 4))
	r.do(newCmd(2, 1, Sell, Limit, 101, 4))
	h := r.b.Hash()
	ev := r.do(newCmd(3, 1, Buy, FOK, 100, 5)) // only 4 available at <=100
	if e, ok := find(ev, EvReject); !ok || e.Code != RejFOKNotFillable || len(ev) != 1 {
		t.Fatalf("want lone FOK reject, got %s", fmtEvents(ev))
	}
	if r.b.Hash() != h {
		t.Fatal("rejected FOK must not change the book")
	}
	ev = r.do(newCmd(3, 2, Buy, FOK, 101, 8)) // 4@100 + 4@101
	if len(fills(ev)) != 2 || r.b.Len() != 0 {
		t.Fatalf("FOK should fill fully: %s", fmtEvents(ev))
	}
}

func TestFOKRejectedWhenSelfTradeWouldBlock(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 10))
	ev := r.do(newCmd(1, 2, Buy, FOK, 100, 5)) // same participant rests there
	if e, ok := find(ev, EvReject); !ok || e.Code != RejFOKNotFillable {
		t.Fatalf("want FOK reject, got %s", fmtEvents(ev))
	}
}

func TestSelfTradePreventionCancelsNewest(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 5)) // id1, participant 1
	r.do(newCmd(2, 1, Sell, Limit, 100, 5)) // id2, participant 2
	ev := r.do(newCmd(2, 2, Buy, Limit, 100, 8))
	// First in queue is participant 1 -> fills 5; next is participant 2's own -> STP cancels remaining 3.
	f := fills(ev)
	if len(f) != 1 || f[0].MakerID != 1 || f[0].Qty != 5 {
		t.Fatalf("fills: %s", fmtEvents(ev))
	}
	c, ok := find(ev, EvCanceled)
	if !ok || c.Code != ReasonSelfTrade || c.Qty != 3 {
		t.Fatalf("want SELF_TRADE cancel of 3: %s", fmtEvents(ev))
	}
	if o, ok := r.b.Order(2); !ok || o.Rem != 5 {
		t.Fatal("resting own order must be untouched")
	}
	if _, ok := r.b.Best(Buy); ok {
		t.Fatal("STP'd remainder must not rest")
	}
}

func TestCancel(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Buy, Limit, 100, 5)) // id 1
	r.do(newCmd(2, 1, Buy, Limit, 100, 5)) // id 2
	ev := r.do(Command{Kind: CmdCancel, Participant: 1, ClientOID: 9, Target: 1})
	c, _ := find(ev, EvCanceled)
	d, _ := find(ev, EvDelta)
	if c.Code != ReasonUser || c.Qty != 5 || d.Qty != 5 {
		t.Fatalf("bad cancel events: %s", fmtEvents(ev))
	}
	// Wrong owner and unknown id are rejected, book unchanged.
	h := r.b.Hash()
	for _, cmd := range []Command{
		{Kind: CmdCancel, Participant: 1, ClientOID: 10, Target: 2}, // not yours
		{Kind: CmdCancel, Participant: 1, ClientOID: 11, Target: 99},
		{Kind: CmdCancel, Participant: 1, ClientOID: 12, Target: 1}, // already gone
	} {
		ev = r.do(cmd)
		if e, ok := find(ev, EvReject); !ok || e.Code != RejUnknownOrder {
			t.Fatalf("want UNKNOWN_ORDER: %s", fmtEvents(ev))
		}
	}
	if r.b.Hash() != h {
		t.Fatal("rejected cancels changed the book")
	}
	r.do(Command{Kind: CmdCancel, Participant: 2, ClientOID: 13, Target: 2})
	if r.b.Len() != 0 {
		t.Fatal("book should be empty; empty level must be removed")
	}
}

func TestReplaceKeepsPriorityOnlyWhenQtyDownSamePrice(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Buy, Limit, 100, 10)) // id1
	r.do(newCmd(2, 1, Buy, Limit, 100, 10)) // id2
	// Reduce id1: keeps first place.
	r.do(Command{Kind: CmdReplace, Participant: 1, ClientOID: 2, Target: 1, Price: 100, Qty: 4})
	ev := r.do(newCmd(3, 1, Sell, Limit, 100, 5))
	f := fills(ev)
	if len(f) != 2 || f[0].MakerID != 1 || f[0].Qty != 4 || f[1].MakerID != 2 || f[1].Qty != 1 {
		t.Fatalf("id1 should keep priority: %s", fmtEvents(ev))
	}
}

func TestReplaceIncreaseLosesPriority(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Buy, Limit, 100, 10)) // id1
	r.do(newCmd(2, 1, Buy, Limit, 100, 10)) // id2
	ev := r.do(Command{Kind: CmdReplace, Participant: 1, ClientOID: 2, Target: 1, Price: 100, Qty: 12})
	ack, _ := find(ev, EvAck)
	if ack.OrderID != 3 { // seq 3 became the new order id
		t.Fatalf("replace must create a new order id, got %d", ack.OrderID)
	}
	ev = r.do(newCmd(3, 1, Sell, Limit, 100, 5))
	if f := fills(ev); len(f) != 1 || f[0].MakerID != 2 {
		t.Fatalf("id2 should now be first: %s", fmtEvents(ev))
	}
}

func TestReplacePriceChangeCanCross(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 105, 5)) // id1
	r.do(newCmd(2, 1, Buy, Limit, 100, 5))  // id2
	ev := r.do(Command{Kind: CmdReplace, Participant: 2, ClientOID: 2, Target: 2, Price: 105, Qty: 5})
	if len(fills(ev)) != 1 || r.b.Len() != 0 {
		t.Fatalf("replaced order should cross and fill: %s", fmtEvents(ev))
	}
}

func TestBadFieldsRejected(t *testing.T) {
	r := newRunner(t)
	for _, c := range []Command{
		newCmd(1, 1, Buy, Limit, 0, 5),
		newCmd(1, 2, Buy, Limit, 100, 0),
		newCmd(1, 3, Buy, Limit, 100, -1),
		newCmd(1, 4, Side(0), Limit, 100, 1),
		newCmd(1, 5, Buy, OrderType(9), 100, 1),
		{Kind: CmdKind(9)},
	} {
		ev := r.do(c)
		if e, ok := find(ev, EvReject); !ok || e.Code != RejBadField || len(ev) != 1 {
			t.Fatalf("%+v: want BAD_FIELD: %s", c, fmtEvents(ev))
		}
	}
	if r.b.Len() != 0 {
		t.Fatal("rejected commands must not change the book")
	}
}

func TestDeltaAfterPartialFillOfLevel(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Sell, Limit, 100, 10))
	ev := r.do(newCmd(2, 1, Buy, Limit, 100, 4))
	d, ok := find(ev, EvDelta)
	if !ok || d.Side != Sell || d.Price != 100 || d.Qty != 6 {
		t.Fatalf("want ask delta 100=6: %s", fmtEvents(ev))
	}
}

func TestSnapshotRoundTripAndHash(t *testing.T) {
	r := newRunner(t)
	for i := 0; i < 50; i++ {
		r.do(newCmd(uint32(i%3)+1, uint64(i), Side(i%2)+1, Limit, Price(90+i%7*3), Qty(i%5+1)))
	}
	snap := r.b.AppendSnapshot(nil)
	b2, n, err := Restore(snap)
	if err != nil || n != len(snap) {
		t.Fatalf("restore: n=%d len=%d err=%v", n, len(snap), err)
	}
	if b2.Hash() != r.b.Hash() {
		t.Fatal("restored hash differs")
	}
	// Same future behaviour from both.
	c := newCmd(9, 1, Buy, Market, 0, 20)
	e1 := r.b.Apply(1000, c, nil)
	e2 := b2.Apply(1000, c, nil)
	if fmtEvents(e1) != fmtEvents(e2) || r.b.Hash() != b2.Hash() {
		t.Fatal("restored book diverged")
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	r := newRunner(t)
	r.do(newCmd(1, 1, Buy, Limit, 100, 5))
	snap := r.b.AppendSnapshot(nil)
	for _, bad := range [][]byte{nil, snap[:5], snap[:len(snap)-1], append(append([]byte{}, snap...), 0)[:len(snap)]} {
		if len(bad) == len(snap) {
			continue
		}
		if _, _, err := Restore(bad); err == nil {
			t.Fatalf("expected error for %d-byte input", len(bad))
		}
	}
	corrupt := append([]byte{}, snap...)
	corrupt[12+12] = 7 // invalid side byte
	if _, _, err := Restore(corrupt); err == nil {
		t.Fatal("expected error for invalid side")
	}
}

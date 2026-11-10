package book

import "sort"

// ref is a deliberately naive matcher used as the oracle in the property test.
// It stores resting orders in a flat slice and finds the best order by linear
// scan. It shares no code with Book, only the types and the documented rules
// (ADR-004). It must stay small enough to audit by reading.
type ref struct {
	resting   []*rorder
	nextTrade uint64
}

type rorder struct {
	id   OrderID
	part uint32
	side Side
	px   Price
	rem  Qty
}

func (r *ref) levelTotal(side Side, px Price) (t Qty) {
	for _, o := range r.resting {
		if o.side == side && o.px == px {
			t += o.rem
		}
	}
	return
}

// bestOpp returns the best resting order against an incoming order of `side`:
// best price first, then lowest id (arrival order).
func (r *ref) bestOpp(side Side) *rorder {
	var best *rorder
	for _, o := range r.resting {
		if o.side == side {
			continue
		}
		if best == nil {
			best = o
			continue
		}
		better := false
		if side == Buy { // taker buys: lowest ask is best
			better = o.px < best.px
		} else {
			better = o.px > best.px
		}
		if o.px == best.px {
			better = o.id < best.id
		}
		if better {
			best = o
		}
	}
	return best
}

func (r *ref) drop(o *rorder) {
	for i, x := range r.resting {
		if x == o {
			r.resting = append(r.resting[:i], r.resting[i+1:]...)
			return
		}
	}
}

func (r *ref) apply(seq uint64, c Command) []Event {
	var out []Event
	rej := func(code uint8) []Event {
		return []Event{{Kind: EvReject, Participant: c.Participant, ClientOID: c.ClientOID, Code: code, Seq: seq}}
	}
	switch c.Kind {
	case CmdNew:
		if !c.Side.Valid() || !c.Type.Valid() || c.Qty <= 0 || (c.Type != Market && c.Price <= 0) {
			return rej(RejBadField)
		}
		if c.Type == FOK {
			// dry run on a sorted copy of the opposing orders
			var opp []*rorder
			for _, o := range r.resting {
				if o.side != c.Side && crosses(c.Side, c.Price, o.px) {
					opp = append(opp, o)
				}
			}
			// A self-trade only blocks the fill if it sits ahead of the quantity we need.
			sort.Slice(opp, func(i, j int) bool {
				a, b := opp[i], opp[j]
				if a.px != b.px {
					if c.Side == Buy {
						return a.px < b.px
					}
					return a.px > b.px
				}
				return a.id < b.id
			})
			ok, sum := true, Qty(0)
			for _, o := range opp {
				if o.part == c.Participant {
					ok = false
					break
				}
				sum += o.rem
				if sum >= c.Qty {
					break
				}
			}
			if !ok || sum < c.Qty {
				return rej(RejFOKNotFillable)
			}
		}
		id := OrderID(seq)
		out = append(out, Event{Kind: EvAck, Code: AckAccepted, OrderID: id, Participant: c.Participant,
			ClientOID: c.ClientOID, Side: c.Side, Price: c.Price, Qty: c.Qty})
		out = r.enter(id, c.Side, c.Type, c.Price, c.Qty, c.Participant, out)
	case CmdCancel:
		o := r.find(c.Target)
		if o == nil || o.part != c.Participant {
			return rej(RejUnknownOrder)
		}
		r.drop(o)
		out = append(out,
			Event{Kind: EvCanceled, Code: ReasonUser, OrderID: o.id, Participant: o.part, Side: o.side, Price: o.px, Qty: o.rem},
			Event{Kind: EvDelta, Side: o.side, Price: o.px, Qty: r.levelTotal(o.side, o.px)})
	case CmdReplace:
		o := r.find(c.Target)
		if o == nil || o.part != c.Participant {
			return rej(RejUnknownOrder)
		}
		if c.Qty <= 0 || c.Price <= 0 {
			return rej(RejBadField)
		}
		if c.Price == o.px && c.Qty <= o.rem {
			o.rem = c.Qty
			out = append(out,
				Event{Kind: EvAck, Code: AckAccepted, OrderID: o.id, Participant: o.part, ClientOID: c.ClientOID, Side: o.side, Price: o.px, Qty: o.rem},
				Event{Kind: EvDelta, Side: o.side, Price: o.px, Qty: r.levelTotal(o.side, o.px)})
			break
		}
		side, px, rem, oid := o.side, o.px, o.rem, o.id
		r.drop(o)
		id := OrderID(seq)
		out = append(out,
			Event{Kind: EvCanceled, Code: ReasonReplaced, OrderID: oid, Participant: c.Participant, Side: side, Price: px, Qty: rem},
			Event{Kind: EvDelta, Side: side, Price: px, Qty: r.levelTotal(side, px)},
			Event{Kind: EvAck, Code: AckAccepted, OrderID: id, Participant: c.Participant, ClientOID: c.ClientOID, Side: side, Price: c.Price, Qty: c.Qty})
		out = r.enter(id, side, Limit, c.Price, c.Qty, c.Participant, out)
	default:
		return rej(RejBadField)
	}
	for i := range out {
		out[i].Seq = seq
		out[i].Idx = uint32(i)
	}
	return out
}

func (r *ref) find(id OrderID) *rorder {
	for _, o := range r.resting {
		if o.id == id {
			return o
		}
	}
	return nil
}

func (r *ref) enter(id OrderID, side Side, typ OrderType, px Price, qty Qty, part uint32, out []Event) []Event {
	rem := qty
	stp := false
	var curPx Price
	curFilled := false
	flush := func() {
		if curFilled {
			out = append(out, Event{Kind: EvDelta, Side: side.Opposite(), Price: curPx, Qty: r.levelTotal(side.Opposite(), curPx)})
		}
		curFilled = false
	}
	for rem > 0 {
		m := r.bestOpp(side)
		if m == nil || (typ != Market && !crosses(side, px, m.px)) {
			break
		}
		if curFilled && m.px != curPx {
			flush()
		}
		if m.part == part {
			stp = true
			break
		}
		q := rem
		if m.rem < q {
			q = m.rem
		}
		r.nextTrade++
		out = append(out, Event{Kind: EvFill, TradeID: r.nextTrade, MakerID: m.id, TakerID: id, Price: m.px, Qty: q,
			Side: side, Participant: part, Counter: m.part})
		m.rem -= q
		rem -= q
		curPx, curFilled = m.px, true
		if m.rem == 0 {
			r.drop(m)
		}
	}
	flush()
	switch {
	case rem == 0:
	case stp:
		out = append(out, Event{Kind: EvCanceled, Code: ReasonSelfTrade, OrderID: id, Participant: part, Side: side, Price: px, Qty: rem})
	case typ == Limit:
		r.resting = append(r.resting, &rorder{id: id, part: part, side: side, px: px, rem: rem})
		out = append(out, Event{Kind: EvDelta, Side: side, Price: px, Qty: r.levelTotal(side, px)})
	case typ == Market:
		out = append(out, Event{Kind: EvCanceled, Code: ReasonNoLiquidity, OrderID: id, Participant: part, Side: side, Price: px, Qty: rem})
	default:
		out = append(out, Event{Kind: EvCanceled, Code: ReasonIOC, OrderID: id, Participant: part, Side: side, Price: px, Qty: rem})
	}
	return out
}

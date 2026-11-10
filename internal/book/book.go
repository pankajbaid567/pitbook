package book

import "sort"

// Order is a resting order. Fields are exported only for tests in this
// package's siblings; callers outside should treat it as read-only.
type Order struct {
	ID          OrderID
	Participant uint32
	Side        Side
	Price       Price
	Rem         Qty

	prev, next *Order
	level      *level
}

type level struct {
	price      Price
	total      Qty
	head, tail *Order
}

func (l *level) push(o *Order) {
	o.level = l
	o.prev, o.next = l.tail, nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.total += o.Rem
}

func (l *level) unlink(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	o.prev, o.next, o.level = nil, nil, nil
}

// bookSide holds price levels sorted from worst to best, so the best level is
// the last element and popping it is O(1). Bids ascend by price, asks descend.
type bookSide struct {
	isBid   bool
	levels  []*level
	byPrice map[Price]*level
}

func newSide(isBid bool) bookSide {
	return bookSide{isBid: isBid, byPrice: make(map[Price]*level)}
}

// worse reports whether price a ranks strictly worse than price b on this side.
func (s *bookSide) worse(a, b Price) bool {
	if s.isBid {
		return a < b
	}
	return a > b
}

func (s *bookSide) best() *level {
	if n := len(s.levels); n > 0 {
		return s.levels[n-1]
	}
	return nil
}

// index returns the position of price p, or where it would be inserted.
func (s *bookSide) index(p Price) int {
	return sort.Search(len(s.levels), func(i int) bool { return !s.worse(s.levels[i].price, p) })
}

func (s *bookSide) getOrCreate(p Price) *level {
	if lv, ok := s.byPrice[p]; ok {
		return lv
	}
	lv := &level{price: p}
	i := s.index(p)
	s.levels = append(s.levels, nil)
	copy(s.levels[i+1:], s.levels[i:])
	s.levels[i] = lv
	s.byPrice[p] = lv
	return lv
}

func (s *bookSide) remove(lv *level) {
	i := s.index(lv.price)
	copy(s.levels[i:], s.levels[i+1:])
	s.levels[len(s.levels)-1] = nil
	s.levels = s.levels[:len(s.levels)-1]
	delete(s.byPrice, lv.price)
}

// Book is a limit order book for one symbol. It is deterministic: Apply is a
// pure function of the book state and its arguments. Not safe for concurrent
// use; the engine owns it from a single goroutine (ADR-006).
type Book struct {
	bids, asks bookSide
	orders     map[OrderID]*Order
	nextTrade  uint64
}

// New returns an empty book.
func New() *Book {
	return &Book{bids: newSide(true), asks: newSide(false), orders: make(map[OrderID]*Order)}
}

func (b *Book) side(s Side) *bookSide {
	if s == Buy {
		return &b.bids
	}
	return &b.asks
}

// Len returns the number of resting orders.
func (b *Book) Len() int { return len(b.orders) }

// NextTradeID returns the number the next trade will get, minus one (the
// count of trades so far).
func (b *Book) Trades() uint64 { return b.nextTrade }

// Order returns a resting order by ID.
func (b *Book) Order(id OrderID) (*Order, bool) {
	o, ok := b.orders[id]
	return o, ok
}

// Best returns the best price on a side and whether the side is non-empty.
func (b *Book) Best(s Side) (Price, bool) {
	if lv := b.side(s).best(); lv != nil {
		return lv.price, true
	}
	return 0, false
}

// Levels returns up to depth aggregated levels of a side, best first.
func (b *Book) Levels(s Side, depth int) []LevelQty {
	sd := b.side(s)
	n := len(sd.levels)
	if depth > n {
		depth = n
	}
	out := make([]LevelQty, 0, depth)
	for i := 0; i < depth; i++ {
		lv := sd.levels[n-1-i]
		out = append(out, LevelQty{Price: lv.price, Qty: lv.total})
	}
	return out
}

func crosses(taker Side, limit, resting Price) bool {
	if taker == Buy {
		return resting <= limit
	}
	return resting >= limit
}

// Apply executes one command at sequence number seq and appends the resulting
// events to out. Events carry Seq and an increasing Idx.
func (b *Book) Apply(seq uint64, c Command, out []Event) []Event {
	start := len(out)
	switch c.Kind {
	case CmdNew:
		out = b.applyNew(seq, c, out)
	case CmdCancel:
		out = b.applyCancel(c, out)
	case CmdReplace:
		out = b.applyReplace(seq, c, out)
	default:
		out = append(out, Event{Kind: EvReject, Participant: c.Participant, ClientOID: c.ClientOID, Code: RejBadField})
	}
	for i := start; i < len(out); i++ {
		out[i].Seq = seq
		out[i].Idx = uint32(i - start)
	}
	return out
}

func reject(c Command, code uint8, out []Event) []Event {
	return append(out, Event{Kind: EvReject, Participant: c.Participant, ClientOID: c.ClientOID, Code: code})
}

func (b *Book) applyNew(seq uint64, c Command, out []Event) []Event {
	if !c.Side.Valid() || !c.Type.Valid() || c.Qty <= 0 || (c.Type != Market && c.Price <= 0) {
		return reject(c, RejBadField, out)
	}
	if c.Type == FOK && !b.canFillFully(c) {
		return reject(c, RejFOKNotFillable, out)
	}
	id := OrderID(seq)
	out = append(out, Event{Kind: EvAck, Code: AckAccepted, OrderID: id, Participant: c.Participant,
		ClientOID: c.ClientOID, Side: c.Side, Price: c.Price, Qty: c.Qty})
	return b.enter(id, c.Side, c.Type, c.Price, c.Qty, c.Participant, c.ClientOID, out)
}

// canFillFully is the FOK dry run: it never mutates the book. A resting order
// of the same participant makes the order unfillable, because self-trade
// prevention would cancel the taker (ADR-004).
func (b *Book) canFillFully(c Command) bool {
	opp := b.side(c.Side.Opposite())
	var sum Qty
	for i := len(opp.levels) - 1; i >= 0; i-- {
		lv := opp.levels[i]
		if !crosses(c.Side, c.Price, lv.price) {
			return false
		}
		for o := lv.head; o != nil; o = o.next {
			if o.Participant == c.Participant {
				return false
			}
			sum += o.Rem
			if sum >= c.Qty {
				return true
			}
		}
	}
	return false
}

// enter matches an incoming order against the book and rests any remainder.
func (b *Book) enter(id OrderID, side Side, typ OrderType, price Price, qty Qty, part uint32, coid uint64, out []Event) []Event {
	rem := qty
	opp := b.side(side.Opposite())
	stp := false
	for rem > 0 && !stp {
		lv := opp.best()
		if lv == nil || (typ != Market && !crosses(side, price, lv.price)) {
			break
		}
		filled := false
		for rem > 0 && lv.head != nil {
			m := lv.head
			if m.Participant == part {
				stp = true
				break
			}
			q := rem
			if m.Rem < q {
				q = m.Rem
			}
			b.nextTrade++
			out = append(out, Event{Kind: EvFill, TradeID: b.nextTrade, MakerID: m.ID, TakerID: id, Price: lv.price,
				Qty: q, Side: side, Participant: part, Counter: m.Participant})
			m.Rem -= q
			lv.total -= q
			rem -= q
			filled = true
			if m.Rem == 0 {
				lv.unlink(m)
				delete(b.orders, m.ID)
			}
		}
		if filled {
			out = append(out, Event{Kind: EvDelta, Side: side.Opposite(), Price: lv.price, Qty: lv.total})
		}
		if lv.head == nil {
			opp.remove(lv)
		}
	}
	switch {
	case rem == 0:
	case stp:
		out = append(out, Event{Kind: EvCanceled, Code: ReasonSelfTrade, OrderID: id, Participant: part, Side: side, Price: price, Qty: rem})
	case typ == Limit:
		lv := b.side(side).getOrCreate(price)
		o := &Order{ID: id, Participant: part, Side: side, Price: price, Rem: rem}
		lv.push(o)
		b.orders[id] = o
		out = append(out, Event{Kind: EvDelta, Side: side, Price: price, Qty: lv.total})
	case typ == Market:
		out = append(out, Event{Kind: EvCanceled, Code: ReasonNoLiquidity, OrderID: id, Participant: part, Side: side, Price: price, Qty: rem})
	default: // IOC, and FOK (unreachable after the dry run)
		out = append(out, Event{Kind: EvCanceled, Code: ReasonIOC, OrderID: id, Participant: part, Side: side, Price: price, Qty: rem})
	}
	return out
}

func (b *Book) remove(o *Order) *level {
	lv := o.level
	lv.total -= o.Rem
	lv.unlink(o)
	delete(b.orders, o.ID)
	if lv.head == nil {
		b.side(o.Side).remove(lv)
	}
	return lv
}

func (b *Book) applyCancel(c Command, out []Event) []Event {
	o, ok := b.orders[c.Target]
	if !ok || o.Participant != c.Participant {
		return reject(c, RejUnknownOrder, out)
	}
	rem, side, price, id := o.Rem, o.Side, o.Price, o.ID
	lv := b.remove(o)
	out = append(out, Event{Kind: EvCanceled, Code: ReasonUser, OrderID: id, Participant: c.Participant, Side: side, Price: price, Qty: rem})
	return append(out, Event{Kind: EvDelta, Side: side, Price: price, Qty: lv.total})
}

func (b *Book) applyReplace(seq uint64, c Command, out []Event) []Event {
	o, ok := b.orders[c.Target]
	if !ok || o.Participant != c.Participant {
		return reject(c, RejUnknownOrder, out)
	}
	if c.Qty <= 0 || c.Price <= 0 {
		return reject(c, RejBadField, out)
	}
	if c.Price == o.Price && c.Qty <= o.Rem {
		// Same price, not larger: keep time priority (ADR-004).
		o.level.total -= o.Rem - c.Qty
		o.Rem = c.Qty
		out = append(out, Event{Kind: EvAck, Code: AckAccepted, OrderID: o.ID, Participant: c.Participant,
			ClientOID: c.ClientOID, Side: o.Side, Price: o.Price, Qty: o.Rem})
		return append(out, Event{Kind: EvDelta, Side: o.Side, Price: o.Price, Qty: o.level.total})
	}
	side, oldPrice, oldRem, oldID := o.Side, o.Price, o.Rem, o.ID
	lv := b.remove(o)
	out = append(out, Event{Kind: EvCanceled, Code: ReasonReplaced, OrderID: oldID, Participant: c.Participant, Side: side, Price: oldPrice, Qty: oldRem})
	out = append(out, Event{Kind: EvDelta, Side: side, Price: oldPrice, Qty: lv.total})
	id := OrderID(seq)
	out = append(out, Event{Kind: EvAck, Code: AckAccepted, OrderID: id, Participant: c.Participant,
		ClientOID: c.ClientOID, Side: side, Price: c.Price, Qty: c.Qty})
	return b.enter(id, side, Limit, c.Price, c.Qty, c.Participant, c.ClientOID, out)
}

package book

// Price is an integer number of ticks. Zero is never a valid limit price.
type Price int64

// Qty is an integer number of lots.
type Qty int64

// OrderID identifies a resting order. It equals the sequence number of the
// command that created the order, so it is unique, increasing and needs no
// extra counter (ADR-002).
type OrderID uint64

// Side of the book.
type Side uint8

const (
	Buy Side = iota + 1
	Sell
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	}
	return "invalid"
}

// Valid reports whether s is Buy or Sell.
func (s Side) Valid() bool { return s == Buy || s == Sell }

// Opposite returns the side an order of side s trades against.
func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

// OrderType selects matching behaviour (ADR-004).
type OrderType uint8

const (
	Limit OrderType = iota + 1
	Market
	IOC // immediate-or-cancel limit
	FOK // fill-or-kill limit
)

func (t OrderType) String() string {
	switch t {
	case Limit:
		return "limit"
	case Market:
		return "market"
	case IOC:
		return "ioc"
	case FOK:
		return "fok"
	}
	return "invalid"
}

// Valid reports whether t is a known order type.
func (t OrderType) Valid() bool { return t >= Limit && t <= FOK }

// CmdKind is the kind of command applied to a book.
type CmdKind uint8

const (
	CmdNew CmdKind = iota + 1
	CmdCancel
	CmdReplace
)

// Command is the deterministic input of the book. It contains no time and no
// randomness; everything the book needs is in here plus the sequence number.
type Command struct {
	Kind        CmdKind
	Side        Side      // New
	Type        OrderType // New
	Participant uint32
	ClientOID   uint64  // client-chosen, unique per participant (idempotency key)
	Price       Price   // New (non-market), Replace
	Qty         Qty     // New, Replace (new open quantity)
	Target      OrderID // Cancel, Replace
}

// EventKind is the kind of a book event.
type EventKind uint8

const (
	EvAck      EventKind = iota + 1 // order accepted
	EvReject                        // command refused, Code says why
	EvFill                          // one trade
	EvCanceled                      // order removed with unfilled quantity, Code says why
	EvDelta                         // new total quantity at one price level
)

// Ack status codes (Event.Code on EvAck).
const (
	AckAccepted  uint8 = 1
	AckDuplicate uint8 = 2
)

// Reject codes (Event.Code on EvReject). The numbers are part of the log
// semantics: do not renumber.
const (
	RejBadField       uint8 = 1
	RejUnknownOrder   uint8 = 2
	RejFOKNotFillable uint8 = 3
)

// Cancel reasons (Event.Code on EvCanceled).
const (
	ReasonUser        uint8 = 1
	ReasonIOC         uint8 = 2
	ReasonNoLiquidity uint8 = 3
	ReasonSelfTrade   uint8 = 4
	ReasonReplaced    uint8 = 5
)

// Event is one output of the book. One flat struct for every kind keeps the
// hot path free of allocation and the encoding trivial. Field use by kind:
//
//	EvAck:      OrderID, Participant, ClientOID, Side, Price, Qty, Code(status)
//	EvReject:   Participant, ClientOID, Code
//	EvFill:     TradeID, MakerID, TakerID, Price, Qty, Side(taker side),
//	            Participant(taker), Counter(maker participant)
//	EvCanceled: OrderID, Participant, Side, Price, Qty(remaining), Code(reason)
//	EvDelta:    Side(of the level), Price, Qty(new level total; 0 = level gone)
type Event struct {
	Kind        EventKind
	Side        Side
	Code        uint8
	Participant uint32
	Counter     uint32
	Seq         uint64 // sequence of the command that produced it
	Idx         uint32 // position among that command's events
	OrderID     OrderID
	MakerID     OrderID
	TakerID     OrderID
	TradeID     uint64
	ClientOID   uint64
	Price       Price
	Qty         Qty
}

// LevelQty is one aggregated price level.
type LevelQty struct {
	Price Price
	Qty   Qty
}

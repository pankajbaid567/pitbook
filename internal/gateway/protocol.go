// Package gateway is the public surface of Pitbook: validation, the JSON
// protocol (docs/protocol.md), market-data fan-out with gap detection and
// resync, and the HTTP/WebSocket server.
package gateway

import (
	"hash/fnv"
	"strings"

	"pitbook/internal/book"
)

// Symbol describes a tradable instrument.
type Symbol struct {
	Name       string `json:"symbol"`
	Tick       int64  `json:"tick"`        // price must be a multiple of Tick (in ticks)
	Lot        int64  `json:"lot"`         // qty must be a multiple of Lot
	PriceScale int64  `json:"price_scale"` // UI divides price by this to display (e.g. 100 = 2 decimals)
}

// Limits keep every number comfortably inside int64 arithmetic.
const (
	MaxPrice = 1_000_000_000_000
	MaxQty   = 1_000_000_000
	MaxIDLen = 32
	maxDepth = 200
	tapeKeep = 200
)

// Gateway-level reject codes (engine-level ones come from the book).
const (
	CodeBadField      = "BAD_FIELD"
	CodeUnknownSymbol = "UNKNOWN_SYMBOL"
	CodeNotTick       = "NOT_TICK_MULTIPLE"
	CodeNotLot        = "NOT_LOT_MULTIPLE"
	CodeUnknownOrder  = "UNKNOWN_ORDER"
	CodeFOK           = "FOK_NOT_FILLABLE"
	CodeBusy          = "ERR_BUSY"
	CodeRateLimited   = "RATE_LIMITED"
	CodeInternal      = "INTERNAL"
)

// ClientMsg is any message a client may send over WebSocket or POST to REST.
type ClientMsg struct {
	Type          string `json:"type"`
	Symbol        string `json:"symbol"`
	ClientOrderID uint64 `json:"client_order_id"`
	ParticipantID string `json:"participant_id"`
	Side          string `json:"side"`
	OrderType     string `json:"order_type"`
	Price         int64  `json:"price"`
	Qty           int64  `json:"qty"`
	TargetOrderID uint64 `json:"target_order_id"`
	Depth         int    `json:"depth"`
}

// ParticipantHash maps a participant string to the engine's numeric id. It is
// a pure function (FNV-32a), so the mapping survives restarts without state.
func ParticipantHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Reject is a validation failure (the command never reaches the engine and
// consumes no sequence number).
type Reject struct {
	Code    string
	Message string
}

func rej(code, msg string) *Reject { return &Reject{Code: code, Message: msg} }

// BuildCommand validates m against the symbol table and converts it to an
// engine command.
func BuildCommand(m ClientMsg, syms map[string]Symbol) (book.Command, Symbol, *Reject) {
	sym, ok := syms[m.Symbol]
	if !ok {
		return book.Command{}, Symbol{}, rej(CodeUnknownSymbol, "unknown symbol")
	}
	if n := len(m.ParticipantID); n == 0 || n > MaxIDLen || strings.TrimSpace(m.ParticipantID) == "" {
		return book.Command{}, sym, rej(CodeBadField, "participant_id must be 1-32 characters")
	}
	c := book.Command{Participant: ParticipantHash(m.ParticipantID), ClientOID: m.ClientOrderID}
	switch m.Type {
	case "NEW":
		c.Kind = book.CmdNew
		switch strings.ToLower(m.Side) {
		case "buy":
			c.Side = book.Buy
		case "sell":
			c.Side = book.Sell
		default:
			return c, sym, rej(CodeBadField, "side must be buy or sell")
		}
		switch strings.ToLower(m.OrderType) {
		case "limit":
			c.Type = book.Limit
		case "market":
			c.Type = book.Market
		case "ioc":
			c.Type = book.IOC
		case "fok":
			c.Type = book.FOK
		default:
			return c, sym, rej(CodeBadField, "order_type must be limit, market, ioc or fok")
		}
		if c.Type != book.Market {
			if m.Price <= 0 || m.Price > MaxPrice {
				return c, sym, rej(CodeBadField, "price must be a positive integer number of ticks")
			}
			if m.Price%sym.Tick != 0 {
				return c, sym, rej(CodeNotTick, "price is not a multiple of the tick")
			}
			c.Price = book.Price(m.Price)
		} else if m.Price != 0 {
			return c, sym, rej(CodeBadField, "market orders take no price")
		}
		if m.Qty <= 0 || m.Qty > MaxQty {
			return c, sym, rej(CodeBadField, "qty must be a positive integer number of lots")
		}
		if m.Qty%sym.Lot != 0 {
			return c, sym, rej(CodeNotLot, "qty is not a multiple of the lot")
		}
		c.Qty = book.Qty(m.Qty)
	case "CANCEL":
		c.Kind = book.CmdCancel
		if m.TargetOrderID == 0 {
			return c, sym, rej(CodeBadField, "target_order_id required")
		}
		c.Target = book.OrderID(m.TargetOrderID)
	case "REPLACE":
		c.Kind = book.CmdReplace
		if m.TargetOrderID == 0 {
			return c, sym, rej(CodeBadField, "target_order_id required")
		}
		if m.Price <= 0 || m.Price > MaxPrice || m.Qty <= 0 || m.Qty > MaxQty {
			return c, sym, rej(CodeBadField, "price and qty must be positive")
		}
		if m.Price%sym.Tick != 0 {
			return c, sym, rej(CodeNotTick, "price is not a multiple of the tick")
		}
		if m.Qty%sym.Lot != 0 {
			return c, sym, rej(CodeNotLot, "qty is not a multiple of the lot")
		}
		c.Target, c.Price, c.Qty = book.OrderID(m.TargetOrderID), book.Price(m.Price), book.Qty(m.Qty)
	default:
		return c, sym, rej(CodeBadField, "unknown message type")
	}
	return c, sym, nil
}

// ---- outbound JSON ----

// EventJSON is the wire form of one engine event.
type EventJSON struct {
	Type          string `json:"type"`
	Seq           uint64 `json:"seq"`
	Idx           uint32 `json:"idx"`
	ClientOrderID uint64 `json:"client_order_id,omitempty"`
	OrderID       uint64 `json:"order_id,omitempty"`
	Status        string `json:"status,omitempty"`
	Code          string `json:"code,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Side          string `json:"side,omitempty"`
	Price         int64  `json:"price,omitempty"`
	Qty           int64  `json:"qty"`
	RemainingQty  int64  `json:"remaining_qty,omitempty"`
	TradeID       uint64 `json:"trade_id,omitempty"`
	MakerOrderID  uint64 `json:"maker_order_id,omitempty"`
	TakerOrderID  uint64 `json:"taker_order_id,omitempty"`
	TakerSide     string `json:"taker_side,omitempty"`
}

func rejectName(c uint8) string {
	switch c {
	case book.RejBadField:
		return CodeBadField
	case book.RejUnknownOrder:
		return CodeUnknownOrder
	case book.RejFOKNotFillable:
		return CodeFOK
	}
	return CodeInternal
}

func reasonName(c uint8) string {
	switch c {
	case book.ReasonUser:
		return "USER"
	case book.ReasonIOC:
		return "IOC_REMAINDER"
	case book.ReasonNoLiquidity:
		return "NO_LIQUIDITY"
	case book.ReasonSelfTrade:
		return "SELF_TRADE"
	case book.ReasonReplaced:
		return "REPLACED"
	}
	return "UNKNOWN"
}

// isPublic reports whether an event belongs on the public feed.
func isPublic(k book.EventKind) bool { return k == book.EvFill || k == book.EvDelta }

// ToJSON converts an engine event. Quantities in BOOK_DELTA are the new level
// total; in FILL the traded quantity.
func ToJSON(e book.Event) EventJSON {
	j := EventJSON{Seq: e.Seq, Idx: e.Idx}
	switch e.Kind {
	case book.EvAck:
		j.Type, j.ClientOrderID, j.OrderID, j.Qty = "ACK", e.ClientOID, uint64(e.OrderID), int64(e.Qty)
		j.Side, j.Price = e.Side.String(), int64(e.Price)
		j.Status = "accepted"
		if e.Code == book.AckDuplicate {
			j.Status = "duplicate"
		}
	case book.EvReject:
		j.Type, j.ClientOrderID, j.Code = "REJECT", e.ClientOID, rejectName(e.Code)
	case book.EvFill:
		j.Type, j.TradeID, j.Price, j.Qty = "FILL", e.TradeID, int64(e.Price), int64(e.Qty)
		j.MakerOrderID, j.TakerOrderID, j.TakerSide = uint64(e.MakerID), uint64(e.TakerID), e.Side.String()
	case book.EvCanceled:
		j.Type, j.OrderID, j.RemainingQty, j.Reason = "CANCELED", uint64(e.OrderID), int64(e.Qty), reasonName(e.Code)
		j.Side, j.Price = e.Side.String(), int64(e.Price)
	case book.EvDelta:
		j.Type, j.Side, j.Price, j.Qty = "BOOK_DELTA", e.Side.String(), int64(e.Price), int64(e.Qty)
	}
	return j
}

// Frame is one public message: the public events of a contiguous run of
// commands (prev_seq, last_seq]. A client applies a frame only if prev_seq
// equals the last sequence it applied; otherwise it has a gap and must resync.
type Frame struct {
	Type    string      `json:"type"`
	Symbol  string      `json:"symbol"`
	PrevSeq uint64      `json:"prev_seq"`
	LastSeq uint64      `json:"last_seq"`
	TsMs    int64       `json:"ts_ms"`
	Events  []EventJSON `json:"events"`
}

// Trade is one entry on the tape.
type Trade struct {
	TradeID   uint64 `json:"trade_id"`
	Seq       uint64 `json:"seq"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	TakerSide string `json:"taker_side"`
	TsMs      int64  `json:"ts_ms"`
}

// Snapshot is the depth-limited book plus the recent tape.
type Snapshot struct {
	Type       string     `json:"type"`
	Symbol     string     `json:"symbol"`
	LastSeq    uint64     `json:"last_seq"`
	PriceScale int64      `json:"price_scale"`
	Bids       [][2]int64 `json:"bids"`
	Asks       [][2]int64 `json:"asks"`
	Trades     []Trade    `json:"trades"`
}

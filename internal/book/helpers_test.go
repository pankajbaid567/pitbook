package book

import (
	"fmt"
	"strings"
	"testing"
)

func newCmd(part uint32, coid uint64, side Side, typ OrderType, price Price, qty Qty) Command {
	return Command{Kind: CmdNew, Participant: part, ClientOID: coid, Side: side, Type: typ, Price: price, Qty: qty}
}

// runner applies commands with an auto-incrementing sequence number and checks
// the book invariants after every command.
type runner struct {
	t   *testing.T
	b   *Book
	seq uint64
}

func newRunner(t *testing.T) *runner { return &runner{t: t, b: New()} }

func (r *runner) do(c Command) []Event {
	r.t.Helper()
	r.seq++
	ev := r.b.Apply(r.seq, c, nil)
	if err := r.b.Check(); err != nil {
		r.t.Fatalf("seq %d: invariant broken: %v\nevents: %s", r.seq, err, fmtEvents(ev))
	}
	return ev
}

func fmtEvents(evs []Event) string {
	var sb strings.Builder
	for _, e := range evs {
		sb.WriteString(fmtEvent(e))
		sb.WriteString("; ")
	}
	return sb.String()
}

func fmtEvent(e Event) string {
	switch e.Kind {
	case EvAck:
		return fmt.Sprintf("ACK(id=%d code=%d q=%d)", e.OrderID, e.Code, e.Qty)
	case EvReject:
		return fmt.Sprintf("REJ(%d)", e.Code)
	case EvFill:
		return fmt.Sprintf("FILL(t=%d maker=%d taker=%d %d@%d)", e.TradeID, e.MakerID, e.TakerID, e.Qty, e.Price)
	case EvCanceled:
		return fmt.Sprintf("CXL(id=%d rem=%d why=%d)", e.OrderID, e.Qty, e.Code)
	case EvDelta:
		return fmt.Sprintf("DELTA(%v %d=%d)", e.Side, e.Price, e.Qty)
	}
	return "?"
}

func fills(evs []Event) (out []Event) {
	for _, e := range evs {
		if e.Kind == EvFill {
			out = append(out, e)
		}
	}
	return
}

func find(evs []Event, k EventKind) (Event, bool) {
	for _, e := range evs {
		if e.Kind == k {
			return e, true
		}
	}
	return Event{}, false
}

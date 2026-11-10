package gateway

import (
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/engine"
)

// client is one subscribed connection.
type client struct {
	send   chan []byte // snapshots and public frames, in order, bounded
	priv   chan []byte // replies to this client's own commands
	resync atomic.Bool
	depth  int
	hub    *hub
}

// hub keeps a depth view of one symbol (rebuilt from BOOK_DELTA events, so the
// engine goroutine never has to answer queries), the recent tape, and the set
// of subscribers. OnEvents runs on the engine goroutine and must never block:
// a subscriber whose queue is full is flagged for resync and skipped.
type hub struct {
	sym Symbol

	mu      sync.Mutex
	bids    map[int64]int64
	asks    map[int64]int64
	lastSeq uint64
	tape    []Trade
	clients map[*client]struct{}

	dropped atomic.Uint64
	frames  atomic.Uint64
	nowMs   func() int64
	capture func(Frame) // optional: sees every frame (used by Recorder)
}

func newHub(s Symbol) *hub {
	return &hub{sym: s, bids: map[int64]int64{}, asks: map[int64]int64{}, clients: map[*client]struct{}{},
		nowMs: func() int64 { return time.Now().UnixMilli() }}
}

// seed rebuilds the view from a recovered machine. Called on the engine goroutine.
func (h *hub) seed(m *engine.Machine) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bids, h.asks = map[int64]int64{}, map[int64]int64{}
	for _, l := range m.Book().Levels(book.Buy, 1<<30) {
		h.bids[int64(l.Price)] = int64(l.Qty)
	}
	for _, l := range m.Book().Levels(book.Sell, 1<<30) {
		h.asks[int64(l.Price)] = int64(l.Qty)
	}
	h.lastSeq = m.LastSeq()
}

func (h *hub) addTrade(e book.Event, ts int64) {
	h.tape = append(h.tape, Trade{TradeID: e.TradeID, Seq: e.Seq, Price: int64(e.Price), Qty: int64(e.Qty),
		TakerSide: e.Side.String(), TsMs: ts})
	if len(h.tape) > tapeKeep {
		h.tape = append(h.tape[:0], h.tape[len(h.tape)-tapeKeep:]...)
	}
}

// replay rebuilds the tape during recovery (no broadcast, no view change: the
// view is seeded from the machine afterwards).
func (h *hub) replay(evs []book.Event) {
	h.mu.Lock()
	for _, e := range evs {
		if e.Kind == book.EvFill {
			h.addTrade(e, 0)
		}
	}
	h.mu.Unlock()
}

func (h *hub) apply(evs []book.Event) {
	if len(evs) == 0 {
		return
	}
	ts := h.nowMs()
	h.mu.Lock()
	defer h.mu.Unlock()
	prev := h.lastSeq
	last := evs[len(evs)-1].Seq
	f := Frame{Type: "EVENTS", Symbol: h.sym.Name, PrevSeq: prev, LastSeq: last, TsMs: ts, Events: make([]EventJSON, 0, len(evs))}
	for _, e := range evs {
		switch e.Kind {
		case book.EvFill:
			h.addTrade(e, ts)
		case book.EvDelta:
			m := h.bids
			if e.Side == book.Sell {
				m = h.asks
			}
			if e.Qty == 0 {
				delete(m, int64(e.Price))
			} else {
				m[int64(e.Price)] = int64(e.Qty)
			}
		}
		if isPublic(e.Kind) {
			f.Events = append(f.Events, ToJSON(e))
		}
	}
	h.lastSeq = last
	h.frames.Add(1)
	if h.capture != nil {
		h.capture(f)
	}
	if len(h.clients) == 0 {
		return
	}
	data, err := json.Marshal(f)
	if err != nil {
		return
	}
	for c := range h.clients {
		select {
		case c.send <- data:
		default:
			c.resync.Store(true)
			h.dropped.Add(1)
		}
	}
}

func topLevels(m map[int64]int64, bid bool, depth int) [][2]int64 {
	px := make([]int64, 0, len(m))
	for p := range m {
		px = append(px, p)
	}
	sort.Slice(px, func(i, j int) bool {
		if bid {
			return px[i] > px[j]
		}
		return px[i] < px[j]
	})
	if len(px) > depth {
		px = px[:depth]
	}
	out := make([][2]int64, len(px))
	for i, p := range px {
		out[i] = [2]int64{p, m[p]}
	}
	return out
}

func (h *hub) snapshotLocked(depth int) Snapshot {
	if depth <= 0 || depth > maxDepth {
		depth = 20
	}
	tr := h.tape
	if len(tr) > 50 {
		tr = tr[len(tr)-50:]
	}
	return Snapshot{Type: "SNAPSHOT", Symbol: h.sym.Name, LastSeq: h.lastSeq, PriceScale: h.sym.PriceScale,
		Bids: topLevels(h.bids, true, depth), Asks: topLevels(h.asks, false, depth), Trades: append([]Trade(nil), tr...)}
}

// Snapshot returns the current depth view.
func (h *hub) Snapshot(depth int) Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked(depth)
}

// enqueueLocked puts msgs at the head of c's (emptied) queue. Caller holds h.mu.
func (h *hub) enqueueLocked(c *client, msgs ...[]byte) {
	for {
		select {
		case <-c.send:
			continue
		default:
		}
		break
	}
	for _, m := range msgs {
		c.send <- m // queue was just emptied and is far larger than len(msgs)
	}
}

// subscribe registers c and queues the snapshot it must start from, atomically
// with respect to apply: the snapshot is the first message in c's queue and the
// next frame c receives has prev_seq == snapshot.LastSeq.
func (h *hub) subscribe(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c.hub = h
	data, _ := json.Marshal(h.snapshotLocked(c.depth))
	h.enqueueLocked(c, data)
	c.resync.Store(false)
	h.clients[c] = struct{}{}
}

func (h *hub) unsubscribe(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// resync discards c's stale queue and queues RESYNC plus a fresh snapshot.
func (h *hub) resync(c *client, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	note, _ := json.Marshal(map[string]string{"type": "RESYNC", "symbol": h.sym.Name, "reason": reason})
	snap, _ := json.Marshal(h.snapshotLocked(c.depth))
	h.enqueueLocked(c, note, snap)
	c.resync.Store(false)
}

func (h *hub) numClients() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

func (h *hub) recentTrades(limit int) []Trade {
	h.mu.Lock()
	defer h.mu.Unlock()
	if limit <= 0 || limit > len(h.tape) {
		limit = len(h.tape)
	}
	out := make([]Trade, 0, limit)
	for i := len(h.tape) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, h.tape[i])
	}
	return out
}

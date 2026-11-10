// Package bots drives the live demo: seeded market makers and noise traders
// that keep a believable book moving. Given the same seed and the same
// sequence of Step calls against a fresh engine, the produced command stream is
// identical (the random source is a seeded PCG and nothing reads the clock).
package bots

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/engine"
)

// Exec runs one command to completion and returns the events it produced.
type Exec func(c book.Command) ([]book.Event, error)

// Config configures a Market.
type Config struct {
	Seed          uint64
	Mid           int64  // starting mid price in ticks
	ClientOIDBase uint64 // first client order id; must not collide with ids used before a restart
	MaxQuotes     int    // live quotes kept per market maker (default 14)
	PartID        func(string) uint32
}

// Market is the simulation state.
type Market struct {
	cfg    Config
	rng    *rand.Rand
	exec   Exec
	mid    int64
	anchor int64
	coid   uint64
	mms    []uint32
	takers []uint32
	quotes map[uint32][]book.OrderID

	Sent, Fills, Errors uint64
}

// New builds a Market that submits through exec.
func New(cfg Config, exec Exec) *Market {
	if cfg.MaxQuotes <= 0 {
		cfg.MaxQuotes = 14
	}
	if cfg.Mid <= 0 {
		cfg.Mid = 10000
	}
	if cfg.PartID == nil {
		cfg.PartID = func(s string) uint32 { return uint32(len(s)) }
	}
	m := &Market{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, 0x5bd1e995)), exec: exec, mid: cfg.Mid, anchor: cfg.Mid,
		coid: cfg.ClientOIDBase, quotes: map[uint32][]book.OrderID{}}
	for _, n := range []string{"mm-alpha", "mm-beta", "mm-gamma"} {
		id := cfg.PartID(n)
		m.mms = append(m.mms, id)
	}
	for _, n := range []string{"flow-1", "flow-2", "flow-3", "flow-4"} {
		m.takers = append(m.takers, cfg.PartID(n))
	}
	return m
}

// Mid returns the simulated fair price in ticks.
func (m *Market) Mid() int64 { return m.mid }

func (m *Market) send(c book.Command) []book.Event {
	m.coid++
	c.ClientOID = m.coid
	evs, err := m.exec(c)
	m.Sent++
	if err != nil {
		m.Errors++
		return nil
	}
	for _, e := range evs {
		if e.Kind == book.EvFill {
			m.Fills++
		}
	}
	return evs
}

// track remembers the order ids a participant may still have resting and
// cancels its oldest ones beyond limit, which keeps the book bounded over a
// long demo.
func (m *Market) track(part uint32, evs []book.Event, limit int) {
	for _, e := range evs {
		if e.Kind == book.EvAck && e.Code == book.AckAccepted {
			q := append(m.quotes[part], e.OrderID)
			for len(q) > limit {
				m.send(book.Command{Kind: book.CmdCancel, Participant: part, Target: q[0]})
				q = q[1:]
			}
			m.quotes[part] = q
		}
	}
}

func (m *Market) quote(part uint32, side book.Side, price int64, qty int64) {
	evs := m.send(book.Command{Kind: book.CmdNew, Participant: part, Side: side, Type: book.Limit, Price: book.Price(price), Qty: book.Qty(qty)})
	m.track(part, evs, m.cfg.MaxQuotes)
}

func (m *Market) takerLimit(part uint32, side book.Side, price int64, qty int64) {
	evs := m.send(book.Command{Kind: book.CmdNew, Participant: part, Side: side, Type: book.Limit, Price: book.Price(price), Qty: book.Qty(qty)})
	m.track(part, evs, 3)
}

// Step performs one action: a price move, a quote, a cancel, or a taker order.
func (m *Market) Step() {
	switch r := m.rng.IntN(100); {
	case r < 8: // fair price drifts, with a pull back toward the anchor
		d := int64(m.rng.IntN(3)) - 1
		if m.mid > m.anchor+250 {
			d = -int64(m.rng.IntN(2))
		} else if m.mid < m.anchor-250 {
			d = int64(m.rng.IntN(2))
		}
		if m.mid+d > 10 {
			m.mid += d
		}
	case r < 46: // a market maker posts liquidity around the mid
		part := m.mms[m.rng.IntN(len(m.mms))]
		off := int64(m.rng.IntN(9)) + 1
		qty := int64(m.rng.IntN(36)) + 5
		if m.rng.IntN(2) == 0 {
			m.quote(part, book.Buy, m.mid-off, qty)
		} else {
			m.quote(part, book.Sell, m.mid+off, qty)
		}
	case r < 58: // a market maker pulls a quote
		part := m.mms[m.rng.IntN(len(m.mms))]
		if q := m.quotes[part]; len(q) > 0 {
			i := m.rng.IntN(len(q))
			m.send(book.Command{Kind: book.CmdCancel, Participant: part, Target: q[i]})
			m.quotes[part] = append(q[:i], q[i+1:]...)
		}
	default: // noise flow takes liquidity
		part := m.takers[m.rng.IntN(len(m.takers))]
		side := book.Buy
		sign := int64(1)
		if m.rng.IntN(2) == 0 {
			side, sign = book.Sell, -1
		}
		qty := int64(m.rng.IntN(15)) + 1
		switch x := m.rng.IntN(100); {
		case x < 55:
			m.takerLimit(part, side, m.mid+sign*int64(m.rng.IntN(4)), qty)
		case x < 75:
			m.send(book.Command{Kind: book.CmdNew, Participant: part, Side: side, Type: book.Market, Qty: book.Qty(qty)})
		case x < 90:
			m.send(book.Command{Kind: book.CmdNew, Participant: part, Side: side, Type: book.IOC, Price: book.Price(m.mid + sign*(int64(m.rng.IntN(4))+1)), Qty: book.Qty(qty)})
		default:
			m.takerLimit(part, side, m.mid-sign*int64(m.rng.IntN(3)), qty)
		}
	}
}

// Run steps at the given rate (steps per second) until ctx is done.
func (m *Market) Run(ctx context.Context, rate float64) {
	if rate <= 0 {
		rate = 20
	}
	t := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Step()
		}
	}
}

// EngineExec adapts an engine to Exec. ErrBusy is reported as an error, which
// the bot counts and ignores.
func EngineExec(e *engine.Engine) Exec {
	return func(c book.Command) ([]book.Event, error) {
		res, err := e.Exec(c, 2*time.Second)
		if err != nil {
			if errors.Is(err, engine.ErrClosed) {
				return nil, err
			}
			return nil, err
		}
		return res.Events, nil
	}
}

package book

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// walk calls fn for every resting order in priority order: bids best to
// worst, then asks best to worst, FIFO within a level.
func (b *Book) walk(fn func(o *Order)) {
	for _, sd := range [...]*bookSide{&b.bids, &b.asks} {
		for i := len(sd.levels) - 1; i >= 0; i-- {
			for o := sd.levels[i].head; o != nil; o = o.next {
				fn(o)
			}
		}
	}
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

type hasher uint64

func newHasher() hasher { return fnvOffset }

func (h *hasher) u64(v uint64) {
	x := uint64(*h)
	for i := 0; i < 8; i++ {
		x ^= v & 0xff
		x *= fnvPrime
		v >>= 8
	}
	*h = hasher(x)
}

// Hash returns a 64-bit FNV-1a digest of the full book state (every resting
// order in priority order plus the trade counter). Two books have the same
// hash iff they would behave identically from here on, barring collisions.
func (b *Book) Hash() uint64 {
	h := newHasher()
	h.u64(b.nextTrade)
	b.walk(func(o *Order) {
		h.u64(uint64(o.ID))
		h.u64(uint64(o.Participant)<<8 | uint64(o.Side))
		h.u64(uint64(o.Price))
		h.u64(uint64(o.Rem))
	})
	return uint64(h)
}

const snapOrderSize = 8 + 4 + 1 + 8 + 8

// AppendSnapshot appends a canonical encoding of the book to dst.
func (b *Book) AppendSnapshot(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, b.nextTrade)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.orders)))
	b.walk(func(o *Order) {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(o.ID))
		dst = binary.LittleEndian.AppendUint32(dst, o.Participant)
		dst = append(dst, byte(o.Side))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(o.Price))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(o.Rem))
	})
	return dst
}

// ErrBadSnapshot is returned by Restore for malformed input.
var ErrBadSnapshot = errors.New("book: bad snapshot")

// Restore rebuilds a book from AppendSnapshot output and returns it together
// with the number of bytes consumed.
func Restore(data []byte) (*Book, int, error) {
	if len(data) < 12 {
		return nil, 0, fmt.Errorf("%w: short header", ErrBadSnapshot)
	}
	b := New()
	b.nextTrade = binary.LittleEndian.Uint64(data)
	n := int(binary.LittleEndian.Uint32(data[8:]))
	off := 12
	if n < 0 || len(data)-off < n*snapOrderSize {
		return nil, 0, fmt.Errorf("%w: truncated orders", ErrBadSnapshot)
	}
	for i := 0; i < n; i++ {
		o := &Order{
			ID:          OrderID(binary.LittleEndian.Uint64(data[off:])),
			Participant: binary.LittleEndian.Uint32(data[off+8:]),
			Side:        Side(data[off+12]),
			Price:       Price(binary.LittleEndian.Uint64(data[off+13:])),
			Rem:         Qty(binary.LittleEndian.Uint64(data[off+21:])),
		}
		off += snapOrderSize
		if !o.Side.Valid() || o.Price <= 0 || o.Rem <= 0 {
			return nil, 0, fmt.Errorf("%w: invalid order %d", ErrBadSnapshot, o.ID)
		}
		if _, dup := b.orders[o.ID]; dup {
			return nil, 0, fmt.Errorf("%w: duplicate order %d", ErrBadSnapshot, o.ID)
		}
		b.side(o.Side).getOrCreate(o.Price).push(o)
		b.orders[o.ID] = o
	}
	if err := b.Check(); err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ErrBadSnapshot, err)
	}
	return b, off, nil
}

// Check verifies every internal invariant and returns the first violation.
// It is O(n) and meant for tests, snapshot loading and debugging.
func (b *Book) Check() error {
	count := 0
	for _, sd := range [...]*bookSide{&b.bids, &b.asks} {
		if len(sd.levels) != len(sd.byPrice) {
			return fmt.Errorf("side bid=%v: %d levels but %d map entries", sd.isBid, len(sd.levels), len(sd.byPrice))
		}
		for i, lv := range sd.levels {
			if i > 0 && !sd.worse(sd.levels[i-1].price, lv.price) {
				return fmt.Errorf("side bid=%v: levels not strictly sorted at %d", sd.isBid, i)
			}
			if sd.byPrice[lv.price] != lv {
				return fmt.Errorf("level %d not in map", lv.price)
			}
			if lv.head == nil {
				return fmt.Errorf("empty level %d kept", lv.price)
			}
			var sum Qty
			var prev *Order
			var lastID OrderID
			for o := lv.head; o != nil; o = o.next {
				if o.prev != prev || o.level != lv || o.Price != lv.price || o.Rem <= 0 {
					return fmt.Errorf("order %d: bad links or fields", o.ID)
				}
				if prev != nil && o.ID <= lastID {
					return fmt.Errorf("level %d: FIFO violated at order %d", lv.price, o.ID)
				}
				if (o.Side == Buy) != sd.isBid {
					return fmt.Errorf("order %d on wrong side", o.ID)
				}
				if b.orders[o.ID] != o {
					return fmt.Errorf("order %d not in index", o.ID)
				}
				sum += o.Rem
				count++
				prev, lastID = o, o.ID
			}
			if lv.tail != prev {
				return fmt.Errorf("level %d: bad tail", lv.price)
			}
			if sum != lv.total {
				return fmt.Errorf("level %d: total %d != sum %d", lv.price, lv.total, sum)
			}
		}
	}
	if count != len(b.orders) {
		return fmt.Errorf("%d orders in levels but %d in index", count, len(b.orders))
	}
	if bb, ok := b.Best(Buy); ok {
		if ba, ok := b.Best(Sell); ok && bb >= ba {
			return fmt.Errorf("crossed book: bid %d >= ask %d", bb, ba)
		}
	}
	return nil
}

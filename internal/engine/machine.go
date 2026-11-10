// Package engine owns one order book per symbol from a single goroutine:
// it assigns sequence numbers, writes commands to the WAL, applies them to the
// deterministic book, and releases acknowledgements only after the covering
// fsync (ADR-006, ADR-007).
package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"pitbook/internal/book"
)

type idemKey struct {
	part uint32
	coid uint64
}

type idemVal struct {
	order book.OrderID // order created (0 when the command was rejected)
	rej   uint8        // reject code, 0 if accepted
}

// Machine is the replicated-state-machine view of one symbol: the book plus
// the idempotency table and the last applied sequence number. Replaying the
// same commands from the same starting state always yields the same Machine
// (ADR-002, ADR-011). Not safe for concurrent use.
type Machine struct {
	bk      *book.Book
	idem    map[idemKey]idemVal
	lastSeq uint64
}

// NewMachine returns an empty machine at sequence 0.
func NewMachine() *Machine {
	return &Machine{bk: book.New(), idem: make(map[idemKey]idemVal)}
}

// LastSeq is the sequence number of the last applied command.
func (m *Machine) LastSeq() uint64 { return m.lastSeq }

// Book returns the underlying book. Callers must not mutate it.
func (m *Machine) Book() *book.Book { return m.bk }

func idempotent(c book.Command) bool { return c.Kind == book.CmdNew || c.Kind == book.CmdReplace }

// Dup reports whether c repeats an earlier New/Replace from the same
// participant with the same client order id, and the events that answer it.
// Duplicates are never sequenced or logged (ADR-011).
func (m *Machine) Dup(c book.Command, out []book.Event) ([]book.Event, bool) {
	if !idempotent(c) {
		return out, false
	}
	v, ok := m.idem[idemKey{c.Participant, c.ClientOID}]
	if !ok {
		return out, false
	}
	if v.rej != 0 {
		return append(out, book.Event{Kind: book.EvReject, Code: v.rej, Participant: c.Participant, ClientOID: c.ClientOID}), true
	}
	return append(out, book.Event{Kind: book.EvAck, Code: book.AckDuplicate, OrderID: v.order, Participant: c.Participant, ClientOID: c.ClientOID}), true
}

// Apply executes c at sequence seq, which must be LastSeq()+1 (or any larger
// value when restoring from a log that starts after a snapshot).
func (m *Machine) Apply(seq uint64, c book.Command, out []book.Event) []book.Event {
	if seq <= m.lastSeq {
		panic(fmt.Sprintf("engine: seq %d not after %d", seq, m.lastSeq))
	}
	start := len(out)
	if dupOut, ok := m.Dup(c, out); ok {
		// Only reachable if a duplicate was logged; answer it the same way.
		m.lastSeq = seq
		for i := start; i < len(dupOut); i++ {
			dupOut[i].Seq, dupOut[i].Idx = seq, uint32(i-start)
		}
		return dupOut
	}
	out = m.bk.Apply(seq, c, out)
	m.lastSeq = seq
	if idempotent(c) {
		// The outcome of the command is its first ACK or REJECT. It is not
		// always the first event: a replace that loses priority emits the
		// CANCELED of the old order first.
		for _, e := range out[start:] {
			if e.Kind == book.EvAck {
				m.idem[idemKey{c.Participant, c.ClientOID}] = idemVal{order: e.OrderID}
				break
			}
			if e.Kind == book.EvReject {
				m.idem[idemKey{c.Participant, c.ClientOID}] = idemVal{rej: e.Code}
				break
			}
		}
	}
	return out
}

// Hash digests the whole machine state: book, last sequence and the
// idempotency table (order-independent over the table).
func (m *Machine) Hash() uint64 {
	var tbl uint64
	for k, v := range m.idem {
		h := fnvNew()
		h = fnvU64(h, uint64(k.part))
		h = fnvU64(h, k.coid)
		h = fnvU64(h, uint64(v.order))
		h = fnvU64(h, uint64(v.rej))
		tbl += h // sum is commutative, so map order does not matter
	}
	h := fnvNew()
	h = fnvU64(h, m.bk.Hash())
	h = fnvU64(h, m.lastSeq)
	h = fnvU64(h, uint64(len(m.idem)))
	return fnvU64(h, tbl)
}

func fnvNew() uint64 { return 14695981039346656037 }

func fnvU64(h, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		h ^= v & 0xff
		h *= 1099511628211
		v >>= 8
	}
	return h
}

// AppendSnapshot encodes the machine canonically (idempotency entries sorted).
func (m *Machine) AppendSnapshot(dst []byte) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, m.lastSeq)
	dst = m.bk.AppendSnapshot(dst)
	keys := make([]idemKey, 0, len(m.idem))
	for k := range m.idem {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].part != keys[j].part {
			return keys[i].part < keys[j].part
		}
		return keys[i].coid < keys[j].coid
	})
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(keys)))
	for _, k := range keys {
		v := m.idem[k]
		dst = binary.LittleEndian.AppendUint32(dst, k.part)
		dst = binary.LittleEndian.AppendUint64(dst, k.coid)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(v.order))
		dst = append(dst, v.rej)
	}
	return dst
}

const idemEntrySize = 4 + 8 + 8 + 1

// RestoreMachine decodes AppendSnapshot output.
func RestoreMachine(data []byte) (*Machine, error) {
	if len(data) < 8 {
		return nil, errors.New("engine: snapshot too short")
	}
	m := &Machine{idem: make(map[idemKey]idemVal)}
	m.lastSeq = binary.LittleEndian.Uint64(data)
	bk, n, err := book.Restore(data[8:])
	if err != nil {
		return nil, err
	}
	m.bk = bk
	rest := data[8+n:]
	if len(rest) < 4 {
		return nil, errors.New("engine: snapshot missing idempotency table")
	}
	cnt := int(binary.LittleEndian.Uint32(rest))
	rest = rest[4:]
	if cnt < 0 || len(rest) != cnt*idemEntrySize {
		return nil, errors.New("engine: snapshot idempotency table has wrong size")
	}
	for i := 0; i < cnt; i++ {
		e := rest[i*idemEntrySize:]
		m.idem[idemKey{binary.LittleEndian.Uint32(e), binary.LittleEndian.Uint64(e[4:])}] =
			idemVal{order: book.OrderID(binary.LittleEndian.Uint64(e[12:])), rej: e[20]}
	}
	return m, nil
}

package engine

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/wal"
)

func genCommand(rng *rand.Rand, coid uint64, issued []book.OrderID) book.Command {
	part := uint32(rng.IntN(5) + 1)
	switch r := rng.IntN(100); {
	case r < 70:
		typ := book.Limit
		switch x := rng.IntN(100); {
		case x > 92:
			typ = book.FOK
		case x > 82:
			typ = book.IOC
		case x > 74:
			typ = book.Market
		}
		c := book.Command{Kind: book.CmdNew, Participant: part, ClientOID: coid, Side: book.Side(rng.IntN(2) + 1),
			Type: typ, Price: book.Price(90 + rng.IntN(21)), Qty: book.Qty(rng.IntN(9) + 1)}
		if typ == book.Market {
			c.Price = 0
		}
		return c
	case r < 88 && len(issued) > 0:
		return book.Command{Kind: book.CmdCancel, Participant: part, ClientOID: coid, Target: issued[rng.IntN(len(issued))]}
	case len(issued) > 0:
		return book.Command{Kind: book.CmdReplace, Participant: part, ClientOID: coid, Target: issued[rng.IntN(len(issued))],
			Price: book.Price(90 + rng.IntN(21)), Qty: book.Qty(rng.IntN(9) + 1)}
	}
	return book.Command{Kind: book.CmdNew, Participant: part, ClientOID: coid, Side: book.Buy, Type: book.Limit, Price: 100, Qty: 1}
}

// do submits one command and waits for its (durable) result, returning a copy.
func do(t testing.TB, e *Engine, c book.Command) Result {
	t.Helper()
	ch := make(chan Result, 1)
	req := &Request{Cmd: c, Done: func(r *Result) {
		cp := *r
		cp.Events = append([]book.Event(nil), r.Events...)
		ch <- cp
	}}
	for {
		err := e.Submit(req)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case r := <-ch:
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for result")
	}
	return Result{}
}

func hashOf(t testing.TB, e *Engine) (uint64, uint64) {
	t.Helper()
	var h, s uint64
	if err := e.Do(func(m *Machine) { h, s = m.Hash(), m.LastSeq() }); err != nil {
		t.Fatal(err)
	}
	return h, s
}

func run(t testing.TB, e *Engine, n int, seed uint64, coidBase uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 99))
	var issued []book.OrderID
	for i := 0; i < n; i++ {
		r := do(t, e, genCommand(rng, coidBase+uint64(i), issued))
		for _, ev := range r.Events {
			if ev.Kind == book.EvAck && ev.Code == book.AckAccepted {
				issued = append(issued, ev.OrderID)
			}
		}
	}
}

func openT(t testing.TB, cfg Config) (*Engine, Recovery) {
	t.Helper()
	e, rec, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e, rec
}

func TestSubmitAssignsSequenceAndAcks(t *testing.T) {
	e, _ := openT(t, Config{Symbol: "T", Dir: t.TempDir()})
	defer e.Close()
	r := do(t, e, book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: 1, Side: book.Buy, Type: book.Limit, Price: 100, Qty: 5})
	if r.Seq != 1 || len(r.Events) != 2 || r.Events[0].Kind != book.EvAck || r.Events[0].OrderID != 1 || r.Events[0].Seq != 1 {
		t.Fatalf("%+v", r)
	}
	r = do(t, e, book.Command{Kind: book.CmdNew, Participant: 2, ClientOID: 1, Side: book.Sell, Type: book.Limit, Price: 100, Qty: 5})
	if r.Seq != 2 {
		t.Fatalf("seq %d", r.Seq)
	}
}

func TestIdempotentRetryReturnsOriginalAndIsNotLogged(t *testing.T) {
	dir := t.TempDir()
	c := book.Command{Kind: book.CmdNew, Participant: 7, ClientOID: 42, Side: book.Buy, Type: book.Limit, Price: 100, Qty: 5}
	e, _ := openT(t, Config{Symbol: "T", Dir: dir})
	first := do(t, e, c)
	again := do(t, e, c)
	if !again.Dup || again.Seq != 0 || len(again.Events) != 1 || again.Events[0].Code != book.AckDuplicate ||
		again.Events[0].OrderID != first.Events[0].OrderID {
		t.Fatalf("dup answer: %+v", again)
	}
	_, seq := hashOf(t, e)
	if seq != 1 {
		t.Fatalf("duplicate consumed a sequence number: %d", seq)
	}
	e.Crash()
	// Retry after a crash and restart: still a duplicate.
	e2, rec := openT(t, Config{Symbol: "T", Dir: dir})
	defer e2.Close()
	if rec.LastSeq != 1 {
		t.Fatalf("recovered to %d", rec.LastSeq)
	}
	if r := do(t, e2, c); !r.Dup || r.Events[0].OrderID != first.Events[0].OrderID {
		t.Fatalf("retry after restart: %+v", r)
	}
	if _, seq := hashOf(t, e2); seq != 1 {
		t.Fatalf("seq %d", seq)
	}
}

func TestRecoveryAfterCrashEqualsLiveState(t *testing.T) {
	dir := t.TempDir()
	e, _ := openT(t, Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync})
	run(t, e, 5000, 1, 1)
	h, s := hashOf(t, e)
	e.Crash()
	e2, rec := openT(t, Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync})
	defer e2.Close()
	if rec.LastSeq != s || rec.Hash != h || rec.Replayed != s {
		t.Fatalf("recovery %+v, live hash %x seq %d", rec, h, s)
	}
}

func TestSnapshotRecoveryEqualsFullReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync, SnapshotEvery: 700, KeepAllWAL: true, SegmentBytes: 16 << 10}
	e, _ := openT(t, cfg)
	run(t, e, 6000, 2, 1)
	h, s := hashOf(t, e)
	e.Crash() // no final snapshot; only the periodic ones exist
	e2, rec := openT(t, cfg)
	if rec.SnapshotSeq == 0 || rec.SnapshotSeq >= s {
		t.Fatalf("expected a mid-run snapshot, got %+v", rec)
	}
	if rec.Hash != h || rec.LastSeq != s {
		t.Fatalf("snapshot+suffix recovery differs: %+v vs %x/%d", rec, h, s)
	}
	e2.Crash()
	full, err := ReplayDir(dir, false, false, nil)
	if err != nil || full.Hash() != h {
		t.Fatalf("full replay from seq 1: hash %x err %v, want %x", full.Hash(), err, h)
	}
	fromSnap, err := ReplayDir(dir, true, false, nil)
	if err != nil || fromSnap.Hash() != h {
		t.Fatalf("snapshot replay: hash %x err %v", fromSnap.Hash(), err)
	}
}

func TestWALSegmentsAreTrimmedAfterSnapshots(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync, SnapshotEvery: 500, SegmentBytes: 8 << 10}
	e, _ := openT(t, cfg)
	run(t, e, 8000, 3, 1)
	h, s := hashOf(t, e)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "wal-*.log"))
	if len(segs) > 12 { // 8000 records * ~57 B = ~450 KB would be ~55 segments untrimmed
		t.Fatalf("segments not trimmed: %d left", len(segs))
	}
	e2, rec := openT(t, cfg)
	defer e2.Close()
	if rec.Hash != h || rec.LastSeq != s {
		t.Fatalf("recovery after trim: %+v want %x/%d", rec, h, s)
	}
}

func TestTornTailLosesOnlyTheTornRecord(t *testing.T) {
	dir := t.TempDir()
	e, _ := openT(t, Config{Symbol: "T", Dir: dir})
	run(t, e, 300, 4, 1)
	e.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "wal-*.log"))
	b, _ := os.ReadFile(segs[len(segs)-1])
	if err := os.WriteFile(segs[len(segs)-1], b[:len(b)-10], 0o644); err != nil {
		t.Fatal(err)
	}
	e2, rec := openT(t, Config{Symbol: "T", Dir: dir})
	defer e2.Close()
	if rec.LastSeq != 299 {
		t.Fatalf("want 299 after losing the torn record, got %d", rec.LastSeq)
	}
	// The engine continues from the right sequence number.
	r := do(t, e2, book.Command{Kind: book.CmdNew, Participant: 9, ClientOID: 1 << 40, Side: book.Buy, Type: book.Limit, Price: 50, Qty: 1})
	if r.Seq != 300 {
		t.Fatalf("next seq %d", r.Seq)
	}
}

func TestMissingRecordsBetweenSnapshotAndLogAreRefused(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync, SnapshotEvery: 400, SegmentBytes: 4 << 10, KeepAllWAL: true}
	e, _ := openT(t, cfg)
	run(t, e, 3000, 5, 1)
	e.Crash()
	// Delete every snapshot and the oldest WAL segment: nothing can rebuild seq 1.
	snaps, _ := filepath.Glob(filepath.Join(dir, "snap", "*"))
	for _, s := range snaps {
		os.Remove(s)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "wal-*.log"))
	os.Remove(segs[0])
	if _, _, err := Open(cfg); err == nil {
		t.Fatal("Open must refuse a log that cannot be replayed from a known state")
	}
}

func TestBackpressureReturnsErrBusy(t *testing.T) {
	gate := make(chan struct{})
	e, _ := openT(t, Config{Symbol: "T", NoWAL: true, QueueSize: 4, MaxBatch: 1})
	defer e.Close()
	first := &Request{Cmd: book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: 1, Side: book.Buy, Type: book.Limit, Price: 1, Qty: 1},
		Done: func(*Result) { <-gate }} // stalls the engine goroutine
	if err := e.Submit(first); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	busy := 0
	for i := 0; i < 20; i++ {
		err := e.Submit(&Request{Cmd: book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: uint64(i + 2), Side: book.Buy, Type: book.Limit, Price: 1, Qty: 1}})
		if errors.Is(err, ErrBusy) {
			busy++
		}
	}
	close(gate)
	if busy != 16 { // queue holds 4
		t.Fatalf("want 16 ErrBusy, got %d", busy)
	}
	if e.Stats().Busy != 16 {
		t.Fatalf("stats busy %d", e.Stats().Busy)
	}
}

func TestCloseDrainsAndAnswersEverythingAccepted(t *testing.T) {
	dir := t.TempDir()
	e, _ := openT(t, Config{Symbol: "T", Dir: dir, QueueSize: 2000})
	var wg sync.WaitGroup
	var mu sync.Mutex
	answered := 0
	accepted := 0
	for i := 0; i < 1500; i++ {
		wg.Add(1)
		err := e.Submit(&Request{Cmd: book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: uint64(i), Side: book.Buy, Type: book.Limit, Price: 10, Qty: 1},
			Done: func(r *Result) { mu.Lock(); answered++; mu.Unlock(); wg.Done() }})
		if err != nil {
			wg.Done()
			continue
		}
		accepted++
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if answered != accepted {
		t.Fatalf("accepted %d answered %d", accepted, answered)
	}
	if err := e.Submit(&Request{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("submit after close: %v", err)
	}
	m, err := ReplayDir(dir, false, false, nil)
	if err != nil || int(m.LastSeq()) != accepted {
		t.Fatalf("log has %d commands, want %d (err %v)", m.LastSeq(), accepted, err)
	}
}

func TestConcurrentSubmittersRace(t *testing.T) {
	e, _ := openT(t, Config{Symbol: "T", Dir: t.TempDir(), WALMode: wal.ModeAsync, Detailed: true})
	defer e.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				do(t, e, book.Command{Kind: book.CmdNew, Participant: uint32(g + 1), ClientOID: uint64(i), Side: book.Side(i%2) + 1,
					Type: book.Limit, Price: book.Price(95 + i%10), Qty: 2})
			}
		}(g)
	}
	wg.Wait()
	st := e.Stats()
	if st.Commands != 3200 || st.LastSeq != 3200 || st.Total.Count() != 3200 {
		t.Fatalf("stats: %+v", st.Commands)
	}
	if st.Batches >= st.Commands {
		t.Logf("no batching observed (batches=%d commands=%d)", st.Batches, st.Commands)
	}
}

func TestOnEventsSeesOnlyDurableBatches(t *testing.T) {
	var seen []uint64
	var mu sync.Mutex
	e, _ := openT(t, Config{Symbol: "T", Dir: t.TempDir(), OnEvents: func(evs []book.Event) {
		mu.Lock()
		for _, ev := range evs {
			if ev.Kind == book.EvAck {
				seen = append(seen, ev.Seq)
			}
		}
		mu.Unlock()
	}})
	run(t, e, 50, 6, 1)
	e.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("OnEvents never called")
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("events out of order: %v", seen)
		}
	}
}

func TestMachineSnapshotRoundTrip(t *testing.T) {
	m := NewMachine()
	rng := rand.New(rand.NewPCG(8, 8))
	var issued []book.OrderID
	var buf []book.Event
	for i := 1; i <= 3000; i++ {
		buf = m.Apply(uint64(i), genCommand(rng, uint64(i), issued), buf[:0])
		for _, e := range buf {
			if e.Kind == book.EvAck {
				issued = append(issued, e.OrderID)
			}
		}
	}
	snap := m.AppendSnapshot(nil)
	m2, err := RestoreMachine(snap)
	if err != nil || m2.Hash() != m.Hash() || m2.LastSeq() != m.LastSeq() {
		t.Fatalf("restore: %v", err)
	}
	if string(m2.AppendSnapshot(nil)) != string(snap) {
		t.Fatal("snapshot encoding is not canonical")
	}
	for _, cut := range []int{0, 7, 20, len(snap) - 1} {
		if _, err := RestoreMachine(snap[:cut]); err == nil {
			t.Fatalf("truncated snapshot (%d bytes) accepted", cut)
		}
	}
}

func TestReplayDeterminism(t *testing.T) {
	dir := t.TempDir()
	e, _ := openT(t, Config{Symbol: "T", Dir: dir, WALMode: wal.ModeAsync})
	run(t, e, 4000, 9, 1)
	h, _ := hashOf(t, e)
	e.Close()
	for i := 0; i < 25; i++ { // Go randomises map iteration; hashes must not care
		m, err := ReplayDir(dir, false, false, nil)
		if err != nil || m.Hash() != h {
			t.Fatalf("replay %d: %x err %v want %x", i, m.Hash(), err, h)
		}
	}
}

// Regression test for a bug the crash harness found: a REPLACE that loses time
// priority starts with a CANCELED event, and its outcome was not recorded in
// the idempotency table, so a retry was sequenced and logged a second time.
func TestReplaceThatLosesPriorityIsIdempotent(t *testing.T) {
	e, _ := openT(t, Config{Symbol: "T", Dir: t.TempDir(), WALMode: wal.ModeAsync})
	defer e.Close()
	do(t, e, book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: 1, Side: book.Buy, Type: book.Limit, Price: 100, Qty: 5})
	rep := book.Command{Kind: book.CmdReplace, Participant: 1, ClientOID: 2, Target: 1, Price: 101, Qty: 5} // price change: loses priority
	first := do(t, e, rep)
	if first.Events[0].Kind != book.EvCanceled {
		t.Fatalf("test premise: replace should start with CANCELED, got %v", first.Events[0].Kind)
	}
	var newID book.OrderID
	for _, ev := range first.Events {
		if ev.Kind == book.EvAck {
			newID = ev.OrderID
		}
	}
	again := do(t, e, rep)
	if !again.Dup || again.Seq != 0 || len(again.Events) != 1 || again.Events[0].Kind != book.EvAck ||
		again.Events[0].Code != book.AckDuplicate || again.Events[0].OrderID != newID {
		t.Fatalf("retry must be a duplicate naming order %d, got %+v", newID, again)
	}
	if _, seq := hashOf(t, e); seq != 2 {
		t.Fatalf("retry consumed a sequence number: %d", seq)
	}
	// The same must hold when the machine is rebuilt from the log.
	m := NewMachine()
	m.Apply(1, book.Command{Kind: book.CmdNew, Participant: 1, ClientOID: 1, Side: book.Buy, Type: book.Limit, Price: 100, Qty: 5}, nil)
	m.Apply(2, rep, nil)
	if _, dup := m.Dup(rep, nil); !dup {
		t.Fatal("idempotency entry missing after replay")
	}
}

package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/metrics"
	"pitbook/internal/snapshot"
	"pitbook/internal/wal"
)

// Errors returned by Submit.
var (
	ErrBusy   = errors.New("engine: queue full")
	ErrClosed = errors.New("engine: closed")
)

// Config configures an Engine for one symbol.
type Config struct {
	Symbol        string
	Dir           string // data directory; ignored when NoWAL
	NoWAL         bool   // benchmark mode: no log, no snapshots, no durability
	WALMode       wal.Mode
	SegmentBytes  int64
	SnapshotEvery uint64 // commands between snapshots; 0 disables snapshots
	KeepAllWAL    bool   // never delete covered segments (lets tools replay from seq 1)
	QueueSize     int    // bounded command queue; default 4096
	MaxBatch      int    // cap on commands per group commit; default 256
	Detailed      bool   // record latency histograms (adds two clock reads per command)

	// OnEvents is called from the engine goroutine once per batch, after the
	// batch is durable, with every event the batch produced. It must not
	// block and must not retain evs. Duplicate answers are not included.
	OnEvents func(evs []book.Event)
	// OnReplay is called during recovery for each replayed command's events.
	OnReplay func(evs []book.Event)
}

// Result is delivered to Request.Done after the command is durable.
type Result struct {
	Seq    uint64 // 0 for duplicates and failures
	Dup    bool
	Err    error
	Events []book.Event // valid only during the Done callback
}

// Request is one command plus its completion callback. Done runs on the engine
// goroutine: it must be quick and must copy anything it keeps.
type Request struct {
	Cmd  book.Command
	Done func(*Result)
	t0   time.Time
}

// Recovery describes what Open did to rebuild state.
type Recovery struct {
	SnapshotSeq uint64
	Replayed    uint64
	LastSeq     uint64
	Skipped     []string // snapshots rejected as damaged
	Took        time.Duration
	Hash        uint64
}

// Stats are cheap counters plus (when Detailed) latency histograms.
type Stats struct {
	Commands uint64
	Batches  uint64
	Dups     uint64
	Busy     uint64
	LastSeq  uint64
	Queue    int
	QueueCap int
	// Histograms (nanoseconds); zero values unless Config.Detailed.
	QueueWait metrics.Histogram // submit to batch start
	Sync      metrics.Histogram // one group commit fsync
	Total     metrics.Histogram // submit to completion callback
	BatchSize metrics.Histogram // commands per batch (recorded as ns units = count)
}

type snapJob struct {
	seq     uint64
	payload []byte
}

// Engine is the single-writer sequencer for one symbol.
type Engine struct {
	cfg Config
	m   *Machine
	log *wal.Log

	mu     sync.RWMutex // guards closed vs. sends on in
	closed bool
	in     chan *Request
	ctl    chan func(*Machine)
	quit   chan struct{}
	done   chan struct{}

	failed      error
	lastSnapSeq uint64
	snapCh      chan snapJob
	snapWG      sync.WaitGroup

	commands, batches, dups, busy atomic.Uint64
	lastSeq                       atomic.Uint64
	smu                           sync.Mutex
	st                            Stats

	// batch scratch, owned by the loop goroutine
	evbuf  []book.Event
	dupbuf []book.Event
	spans  []span
	enc    []byte
}

type span struct {
	a, b int
	seq  uint64
	dup  bool
}

func (c Config) walDir() string  { return filepath.Join(c.Dir, "wal") }
func (c Config) snapDir() string { return filepath.Join(c.Dir, "snap") }

// Open recovers state from the data directory (snapshot, then log suffix) and
// starts the engine goroutine.
func Open(cfg Config) (*Engine, Recovery, error) {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if !cfg.NoWAL && cfg.Dir == "" {
		return nil, Recovery{}, errors.New("engine: Dir required unless NoWAL")
	}
	t0 := time.Now()
	e := &Engine{
		cfg:  cfg,
		in:   make(chan *Request, cfg.QueueSize),
		ctl:  make(chan func(*Machine), 8),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	var rec Recovery
	e.m = NewMachine()
	if !cfg.NoWAL {
		if err := e.recover(&rec); err != nil {
			if e.log != nil {
				e.log.Close()
			}
			return nil, rec, err
		}
	}
	rec.LastSeq = e.m.LastSeq()
	rec.Hash = e.m.Hash()
	rec.Took = time.Since(t0)
	e.lastSeq.Store(rec.LastSeq)
	e.lastSnapSeq = rec.SnapshotSeq
	e.st.QueueCap = cfg.QueueSize
	if cfg.SnapshotEvery > 0 && !cfg.NoWAL {
		e.snapCh = make(chan snapJob, 1)
		e.snapWG.Add(1)
		go e.snapshotter()
	}
	go e.loop()
	return e, rec, nil
}

func (e *Engine) recover(rec *Recovery) error {
	cfg := e.cfg
	seq, payload, skipped, err := snapshot.Latest(cfg.snapDir())
	rec.Skipped = skipped
	switch {
	case err == nil:
		m, rerr := RestoreMachine(payload)
		if rerr != nil {
			return fmt.Errorf("engine: snapshot %d: %w", seq, rerr)
		}
		if m.LastSeq() != seq {
			return fmt.Errorf("engine: snapshot %d claims sequence %d", seq, m.LastSeq())
		}
		e.m, rec.SnapshotSeq = m, seq
	case errors.Is(err, snapshot.ErrNone):
	default:
		return err
	}
	log, err := wal.Open(wal.Options{Dir: cfg.walDir(), SegmentBytes: cfg.SegmentBytes, Mode: cfg.WALMode})
	if err != nil {
		return err
	}
	e.log = log
	switch {
	case log.FirstSeq() == 0: // empty log
		if err := log.Rebase(rec.SnapshotSeq); err != nil {
			return err
		}
	case log.LastSeq() < rec.SnapshotSeq:
		return fmt.Errorf("engine: log ends at %d, before snapshot %d", log.LastSeq(), rec.SnapshotSeq)
	case log.FirstSeq() > rec.SnapshotSeq+1:
		return fmt.Errorf("engine: log starts at %d but snapshot is at %d: records %d..%d are missing",
			log.FirstSeq(), rec.SnapshotSeq, rec.SnapshotSeq+1, log.FirstSeq()-1)
	}
	var evs []book.Event
	last, err := wal.Replay(cfg.walDir(), rec.SnapshotSeq, false, func(seq uint64, typ byte, body []byte) error {
		if typ != RecCommand {
			return fmt.Errorf("engine: record %d has unknown type %d", seq, typ)
		}
		c, err := DecodeCommand(body)
		if err != nil {
			return fmt.Errorf("engine: record %d: %w", seq, err)
		}
		evs = e.m.Apply(seq, c, evs[:0])
		rec.Replayed++
		if cfg.OnReplay != nil {
			cfg.OnReplay(evs)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if last != e.m.LastSeq() || (rec.Replayed > 0 && last != log.LastSeq()) {
		return fmt.Errorf("engine: replay ended at %d, log ends at %d, machine at %d", last, log.LastSeq(), e.m.LastSeq())
	}
	return nil
}

// Submit queues a command. It never blocks: a full queue returns ErrBusy.
func (e *Engine) Submit(r *Request) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if e.cfg.Detailed {
		r.t0 = time.Now()
	}
	select {
	case e.in <- r:
		return nil
	default:
		e.busy.Add(1)
		return ErrBusy
	}
}

// Do runs fn on the engine goroutine with exclusive access to the machine and
// waits for it. It is how other goroutines read state safely (depth, hash).
func (e *Engine) Do(fn func(*Machine)) error {
	ch := make(chan struct{})
	wrapped := func(m *Machine) { fn(m); close(ch) }
	select {
	case e.ctl <- wrapped:
	case <-e.done:
		return ErrClosed
	}
	select {
	case <-ch:
		return nil
	case <-e.done:
		return ErrClosed
	}
}

// Stats returns a consistent copy of the counters and histograms.
func (e *Engine) Stats() Stats {
	e.smu.Lock()
	s := e.st
	e.smu.Unlock()
	s.Commands, s.Batches, s.Dups, s.Busy = e.commands.Load(), e.batches.Load(), e.dups.Load(), e.busy.Load()
	s.LastSeq = e.lastSeq.Load()
	s.Queue = len(e.in)
	return s
}

// Symbol returns the configured symbol.
func (e *Engine) Symbol() string { return e.cfg.Symbol }

// Close stops accepting commands, drains what is queued (all of it becomes
// durable and is answered), writes a final snapshot if enabled, and closes
// the log.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	close(e.quit)
	<-e.done
	if e.snapCh != nil {
		close(e.snapCh)
		e.snapWG.Wait()
	}
	var err error
	if e.cfg.SnapshotEvery > 0 && !e.cfg.NoWAL && e.m.LastSeq() > e.lastSnapSeq && e.failed == nil {
		if _, werr := snapshot.Write(e.cfg.snapDir(), e.m.LastSeq(), e.m.AppendSnapshot(nil)); werr != nil {
			err = werr
		}
	}
	if e.log != nil {
		if cerr := e.log.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// Crash stops the engine without draining, snapshotting or syncing, closing
// file descriptors only. It simulates a process crash in tests.
func (e *Engine) Crash() {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	close(e.quit)
	<-e.done
	if e.snapCh != nil {
		close(e.snapCh)
		e.snapWG.Wait()
	}
	if e.log != nil {
		e.log.CloseNoFlush()
	}
}

func (e *Engine) loop() {
	defer close(e.done)
	batch := make([]*Request, 0, e.cfg.MaxBatch)
	for {
		select {
		case r := <-e.in:
			batch = append(batch[:0], r)
			batch = e.fill(batch)
			e.process(batch)
		case fn := <-e.ctl:
			fn(e.m)
		case <-e.quit:
			select { // drain everything that was accepted before Close
			case r := <-e.in:
				batch = append(batch[:0], r)
				batch = e.fill(batch)
				e.process(batch)
				continue
			default:
			}
			return
		}
	}
}

// fill adds already-queued requests to the batch without waiting.
func (e *Engine) fill(batch []*Request) []*Request {
	for len(batch) < e.cfg.MaxBatch {
		select {
		case r := <-e.in:
			batch = append(batch, r)
		default:
			return batch
		}
	}
	return batch
}

func (e *Engine) process(batch []*Request) {
	var tStart time.Time
	if e.cfg.Detailed {
		tStart = time.Now()
	}
	if e.failed != nil {
		for _, r := range batch {
			if r.Done != nil {
				r.Done(&Result{Err: e.failed})
			}
		}
		return
	}
	e.evbuf, e.dupbuf, e.spans = e.evbuf[:0], e.dupbuf[:0], e.spans[:0]
	var ndup uint64
	for _, r := range batch {
		if d, ok := e.m.Dup(r.Cmd, e.dupbuf); ok {
			a := len(e.dupbuf)
			e.dupbuf = d
			e.spans = append(e.spans, span{a: a, b: len(d), dup: true})
			ndup++
			continue
		}
		seq := e.m.LastSeq() + 1
		if e.log != nil {
			e.enc = EncodeCommand(e.enc[:0], r.Cmd)
			if err := e.log.Append(seq, RecCommand, e.enc); err != nil {
				e.fail(err, batch)
				return
			}
		}
		a := len(e.evbuf)
		e.evbuf = e.m.Apply(seq, r.Cmd, e.evbuf)
		e.spans = append(e.spans, span{a: a, b: len(e.evbuf), seq: seq})
	}
	var syncNS time.Duration
	if e.log != nil {
		var ts time.Time
		if e.cfg.Detailed {
			ts = time.Now()
		}
		if err := e.log.Sync(); err != nil {
			e.fail(err, batch)
			return
		}
		if e.cfg.Detailed {
			syncNS = time.Since(ts)
		}
	}
	// Durable from here on: publish, then answer.
	e.lastSeq.Store(e.m.LastSeq())
	if e.cfg.OnEvents != nil && len(e.evbuf) > 0 {
		e.cfg.OnEvents(e.evbuf)
	}
	var res Result
	for i, r := range batch {
		sp := e.spans[i]
		if r.Done != nil {
			if sp.dup {
				res = Result{Dup: true, Events: e.dupbuf[sp.a:sp.b]}
			} else {
				res = Result{Seq: sp.seq, Events: e.evbuf[sp.a:sp.b]}
			}
			r.Done(&res)
		}
	}
	e.commands.Add(uint64(len(batch)))
	e.batches.Add(1)
	e.dups.Add(ndup)
	if e.cfg.Detailed {
		now := time.Now()
		e.smu.Lock()
		e.st.Sync.Record(syncNS)
		e.st.BatchSize.Record(time.Duration(len(batch)))
		for _, r := range batch {
			e.st.QueueWait.Record(tStart.Sub(r.t0))
			e.st.Total.Record(now.Sub(r.t0))
		}
		e.smu.Unlock()
	}
	if e.snapCh != nil && e.m.LastSeq()-e.lastSnapSeq >= e.cfg.SnapshotEvery {
		select {
		case e.snapCh <- snapJob{seq: e.m.LastSeq(), payload: e.m.AppendSnapshot(nil)}:
			e.lastSnapSeq = e.m.LastSeq()
		default: // previous snapshot still being written; try after the next batch
		}
	}
}

func (e *Engine) fail(err error, batch []*Request) {
	e.failed = fmt.Errorf("engine %s stopped: %w", e.cfg.Symbol, err)
	for _, r := range batch {
		if r.Done != nil {
			r.Done(&Result{Err: e.failed})
		}
	}
}

// snapshotter writes snapshots off the matching goroutine, prunes old ones and
// asks the loop to drop log segments the snapshot covers.
func (e *Engine) snapshotter() {
	defer e.snapWG.Done()
	for job := range e.snapCh {
		if _, err := snapshot.Write(e.cfg.snapDir(), job.seq, job.payload); err != nil {
			fmt.Fprintf(os.Stderr, "engine %s: snapshot %d failed: %v\n", e.cfg.Symbol, job.seq, err)
			continue
		}
		_ = snapshot.Prune(e.cfg.snapDir(), 2)
		if e.cfg.KeepAllWAL {
			continue
		}
		seq := job.seq
		select {
		case e.ctl <- func(*Machine) {
			if e.log != nil {
				_ = e.log.TruncateBefore(seq)
			}
		}:
		case <-e.done:
		}
	}
}

// ReplayDir rebuilds the machine from a data directory without opening it for
// writing and without modifying any file. With fromSnapshot it starts from the
// newest valid snapshot, otherwise from sequence 1 (which requires the log to
// start at 1). tolerateTornTail ignores a damaged last record.
func ReplayDir(dir string, fromSnapshot, tolerateTornTail bool, onEvents func(evs []book.Event)) (*Machine, error) {
	m := NewMachine()
	var after uint64
	if fromSnapshot {
		seq, payload, _, err := snapshot.Latest(filepath.Join(dir, "snap"))
		if err == nil {
			if m, err = RestoreMachine(payload); err != nil {
				return nil, err
			}
			after = seq
		} else if !errors.Is(err, snapshot.ErrNone) {
			return nil, err
		}
	}
	var evs []book.Event
	_, err := wal.Replay(filepath.Join(dir, "wal"), after, tolerateTornTail, func(seq uint64, typ byte, body []byte) error {
		if typ != RecCommand {
			return fmt.Errorf("record %d: unknown type %d", seq, typ)
		}
		c, err := DecodeCommand(body)
		if err != nil {
			return fmt.Errorf("record %d: %w", seq, err)
		}
		evs = m.Apply(seq, c, evs[:0])
		if onEvents != nil {
			onEvents(evs)
		}
		return nil
	})
	return m, err
}

// Exec submits c and waits for its durable result, returning a private copy.
// Convenience for bots, tools and the HTTP/WebSocket handlers; the hot path
// uses Submit with a callback.
func (e *Engine) Exec(c book.Command, timeout time.Duration) (Result, error) {
	ch := make(chan Result, 1)
	err := e.Submit(&Request{Cmd: c, Done: func(r *Result) {
		cp := *r
		cp.Events = append([]book.Event(nil), r.Events...)
		ch <- cp
	}})
	if err != nil {
		return Result{}, err
	}
	select {
	case r := <-ch:
		return r, r.Err
	case <-time.After(timeout):
		return Result{}, errors.New("engine: timed out waiting for result")
	}
}

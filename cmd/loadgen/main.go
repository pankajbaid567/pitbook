// Command loadgen is the open-loop benchmark driver (docs/benchmarks-method.md).
//
// It drives the engine in-process on a fixed schedule: command i is *due* at
// start + i/rate whether or not earlier commands have finished, and latency is
// measured from the due time to the durable completion callback. A closed loop
// (send, wait, send) would hide queueing delay, so rate=0 ("saturation") is
// reported as throughput only and never as latency.
//
// Every run appends one row to a CSV in bench/results; BENCHMARKS.md is
// generated from those rows by scripts/make_benchmarks.py.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/engine"
	"pitbook/internal/metrics"
	"pitbook/internal/wal"
)

var (
	mode      = flag.String("mode", "engine", "engine (no log) | wal-async | wal-sync")
	workload  = flag.String("workload", "steady", "steady | deep | burst | multi | recovery")
	rate      = flag.Float64("rate", 50000, "target commands/s across all symbols (0 = saturation: throughput only)")
	duration  = flag.Duration("duration", 30*time.Second, "measured window")
	warmup    = flag.Duration("warmup", 5*time.Second, "warm-up before the measured window")
	reps      = flag.Int("reps", 1, "repetitions (a fresh engine each)")
	seed      = flag.Uint64("seed", 1, "workload seed (rep r uses seed+r)")
	out       = flag.String("out", "bench/results/loadgen.csv", "CSV file to append rows to")
	nRecovery = flag.Int("n", 1_000_000, "recovery workload: commands in the log")
	symbolsN  = flag.Int("symbols", 8, "multi workload: number of symbols")
	maxBatch  = flag.Int("max-batch", 256, "engine group-commit cap")
	note      = flag.String("note", "", "free text stored with the row (e.g. power state)")
)

func main() {
	flag.Parse()
	switch *workload {
	case "recovery":
		runRecovery()
		return
	case "steady", "deep", "burst", "multi":
	default:
		fatalf("unknown workload %q", *workload)
	}
	if *mode != "engine" && *mode != "wal-async" && *mode != "wal-sync" {
		fatalf("unknown mode %q", *mode)
	}
	for r := 0; r < *reps; r++ {
		res := runOnce(*seed + uint64(r))
		res.report()
		appendCSV(res)
	}
}

func fatalf(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...); os.Exit(2) }

// ---- request pooling: no allocation per command on the load side ----

// perSymbol keeps a histogram per engine goroutine (single writer each).
type symState struct {
	hist      metrics.Histogram
	measureAt time.Time
	completed uint64
	rejects   uint64
}

type reqP struct {
	req engine.Request
	due time.Time
	st  *symState
}

var reqPool sync.Pool

func init() {
	reqPool.New = newReqP
}

func newReqP() any {
	p := &reqP{}
	p.req.Done = func(r *engine.Result) {
		st := p.st
		if !p.due.Before(st.measureAt) {
			st.hist.Record(time.Since(p.due))
			st.completed++
			for _, e := range r.Events {
				if e.Kind == book.EvReject {
					st.rejects++
					break
				}
			}
		}
		reqPool.Put(p)
	}
	return p
}

// ---- workload ----

type liveOrder struct {
	id   book.OrderID
	part uint32
}

type gen struct {
	rng    *rand.Rand
	deep   bool
	live   []liveOrder
	seq    uint64 // seq of the next command (single submitter, one seq per command)
	coid   uint64
	mid    int64
	spread int64
	nParts uint32
}

func newGen(seed uint64, deep bool) *gen {
	g := &gen{rng: rand.New(rand.NewPCG(seed, 0xbe9c)), deep: deep, seq: 1, mid: 10000, nParts: 64}
	g.spread = 100
	if deep {
		g.spread = 5000
	}
	return g
}

func (g *gen) next() book.Command {
	g.coid++
	part := uint32(g.rng.IntN(int(g.nParts))) + 1
	switch r := g.rng.IntN(100); {
	case r < 60: // passive limit, away from the touch
		side := book.Buy
		px := g.mid - 1 - int64(g.rng.IntN(int(g.spread)))
		if g.rng.IntN(2) == 0 {
			side, px = book.Sell, g.mid+1+int64(g.rng.IntN(int(g.spread)))
		}
		g.live = append(g.live, liveOrder{book.OrderID(g.seq), part})
		g.seq++
		return book.Command{Kind: book.CmdNew, Participant: part, ClientOID: g.coid, Side: side, Type: book.Limit, Price: book.Price(px), Qty: book.Qty(g.rng.IntN(20) + 1)}
	case r < 85 && len(g.live) > 0: // cancel a (probably) live order
		i := g.rng.IntN(len(g.live))
		o := g.live[i]
		g.live[i] = g.live[len(g.live)-1]
		g.live = g.live[:len(g.live)-1]
		g.seq++
		return book.Command{Kind: book.CmdCancel, Participant: o.part, ClientOID: g.coid, Target: o.id}
	default: // aggressive: crosses the touch a few levels
		side := book.Buy
		px := g.mid + int64(g.rng.IntN(6))
		if g.rng.IntN(2) == 0 {
			side, px = book.Sell, g.mid-int64(g.rng.IntN(6))
		}
		typ := book.Limit
		if g.rng.IntN(3) == 0 {
			typ, px = book.Market, 0
		}
		if typ == book.Limit { // a limit may rest if it does not fully fill
			g.live = append(g.live, liveOrder{book.OrderID(g.seq), part})
		}
		g.seq++
		return book.Command{Kind: book.CmdNew, Participant: part, ClientOID: g.coid, Side: side, Type: typ, Price: book.Price(px), Qty: book.Qty(g.rng.IntN(15) + 1)}
	}
}

func (g *gen) preload(e *engine.Engine, n int) {
	for i := 0; i < n; i++ {
		g.coid++
		part := uint32(g.rng.IntN(int(g.nParts))) + 1
		side := book.Buy
		px := g.mid - 1 - int64(g.rng.IntN(int(g.spread)))
		if i%2 == 1 {
			side, px = book.Sell, g.mid+1+int64(g.rng.IntN(int(g.spread)))
		}
		cmd := book.Command{Kind: book.CmdNew, Participant: part, ClientOID: g.coid, Side: side, Type: book.Limit, Price: book.Price(px), Qty: book.Qty(g.rng.IntN(20) + 1)}
		for e.Submit(&engine.Request{Cmd: cmd}) != nil {
			runtime.Gosched()
		}
		g.live = append(g.live, liveOrder{book.OrderID(g.seq), part})
		g.seq++
	}
	for e.Stats().LastSeq < g.seq-1 {
		time.Sleep(time.Millisecond)
	}
}

// ---- one run ----

type result struct {
	runID, ts, mode, workload string
	seed                      uint64
	targetRate                float64
	dur, warm                 time.Duration
	completed, dropped        uint64
	rejects                   uint64
	throughput                float64
	p50, p99, p999, maxv      float64
	samples                   int64
	batches, commands         uint64
	allocsPerCmd              float64
	gcCycles                  uint32
	gcPauseMs                 float64
	symbols                   int
}

func runOnce(seed uint64) result {
	nSym := 1
	if *workload == "multi" {
		nSym = *symbolsN
	}
	deep := *workload == "deep"
	preload := 5000
	if deep {
		preload = 100000
	}
	dirs := make([]string, nSym)
	engines := make([]*engine.Engine, nSym)
	states := make([]*symState, nSym)
	gens := make([]*gen, nSym)
	for i := range engines {
		cfg := engine.Config{Symbol: fmt.Sprint("S", i), QueueSize: 1 << 16, MaxBatch: *maxBatch}
		switch *mode {
		case "engine":
			cfg.NoWAL = true
		case "wal-async", "wal-sync":
			d, err := os.MkdirTemp("", "pitbook-bench-*")
			if err != nil {
				fatalf("%v", err)
			}
			dirs[i], cfg.Dir = d, d
			cfg.SnapshotEvery = 0
			if *mode == "wal-async" {
				cfg.WALMode = wal.ModeAsync
			}
		}
		e, _, err := engine.Open(cfg)
		if err != nil {
			fatalf("open: %v", err)
		}
		engines[i], states[i] = e, &symState{}
		gens[i] = newGen(seed+uint64(i)*7919, deep)
		gens[i].preload(e, preload)
	}
	defer func() {
		for i, e := range engines {
			e.Close()
			if dirs[i] != "" {
				os.RemoveAll(dirs[i])
			}
		}
	}()

	perSymRate := *rate / float64(nSym)
	start := time.Now().Add(50 * time.Millisecond)
	measureAt := start.Add(*warmup)
	end := measureAt.Add(*duration)
	for _, st := range states {
		st.measureAt = measureAt
	}
	var dropped atomic.Uint64
	var wg sync.WaitGroup
	var msBefore, msAfter runtime.MemStats
	var cmdBefore, cmdAfter, batchBefore, batchAfter uint64
	statsAt := func(before bool) {
		var c, b uint64
		for _, e := range engines {
			s := e.Stats()
			c, b = c+s.Commands, b+s.Batches
		}
		if before {
			runtime.ReadMemStats(&msBefore)
			cmdBefore, batchBefore = c, b
		} else {
			runtime.ReadMemStats(&msAfter)
			cmdAfter, batchAfter = c, b
		}
	}
	go func() {
		time.Sleep(time.Until(measureAt))
		statsAt(true)
	}()
	for i := range engines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, g, st := engines[i], gens[i], states[i]
			var n uint64
			for {
				var due time.Time
				if *rate > 0 {
					if *workload == "burst" {
						due = burstDue(start, n, perSymRate)
					} else {
						due = start.Add(time.Duration(float64(n) / perSymRate * float64(time.Second)))
					}
					if due.After(end) {
						return
					}
					for {
						d := time.Until(due)
						if d <= 0 {
							break
						}
						if d > 2*time.Millisecond {
							time.Sleep(d - time.Millisecond)
						} else {
							runtime.Gosched()
						}
					}
				} else {
					due = time.Now()
					if due.After(end) {
						return
					}
				}
				n++
				p := reqPool.Get().(*reqP)
				p.st, p.due, p.req.Cmd = st, due, g.next()
				for {
					err := e.Submit(&p.req)
					if err == nil {
						break
					}
					if *rate > 0 { // open loop: overload is a drop, not a wait
						if !due.Before(measureAt) {
							dropped.Add(1)
						}
						reqPool.Put(p)
						// the command never ran, so the generator's id prediction must skip it
						g.seq--
						break
					}
					runtime.Gosched() // saturation mode waits for room
				}
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond) // let in-flight commands complete
	statsAt(false)

	var h metrics.Histogram
	var completed, rejects uint64
	for _, st := range states {
		h.Merge(&st.hist)
		completed += st.completed
		rejects += st.rejects
	}
	res := result{
		runID: fmt.Sprintf("%s-%s-%d", *mode, *workload, time.Now().UnixNano()), ts: time.Now().UTC().Format(time.RFC3339),
		mode: *mode, workload: *workload, seed: seed, targetRate: *rate, dur: *duration, warm: *warmup,
		completed: completed, dropped: dropped.Load(), rejects: rejects, symbols: nSym,
		throughput: float64(completed) / duration.Seconds(), samples: h.Count(),
		p50: float64(h.PercentileNS(50)) / 1000, p99: float64(h.PercentileNS(99)) / 1000, p999: float64(h.PercentileNS(99.9)) / 1000, maxv: float64(h.MaxNS()) / 1000,
		batches: batchAfter - batchBefore, commands: cmdAfter - cmdBefore,
		gcCycles: msAfter.NumGC - msBefore.NumGC, gcPauseMs: float64(msAfter.PauseTotalNs-msBefore.PauseTotalNs) / 1e6,
	}
	if res.commands > 0 {
		res.allocsPerCmd = float64(msAfter.Mallocs-msBefore.Mallocs) / float64(res.commands)
	}
	return res
}

// burstDue returns the due time of command n when odd seconds run at rate and
// even seconds at rate/10, computed by walking whole seconds.
func burstDue(start time.Time, n uint64, rate float64) time.Time {
	remaining := float64(n)
	sec := 0
	for {
		r := rate
		if sec%2 == 0 {
			r = rate / 10
		}
		if remaining < r {
			return start.Add(time.Duration(float64(sec)*float64(time.Second) + remaining/r*float64(time.Second)))
		}
		remaining -= r
		sec++
	}
}

func (r result) report() {
	fmt.Printf("%-10s %-7s target=%-8.0f completed=%-9d dropped=%-7d throughput=%9.0f/s  p50=%8.1fus p99=%9.1fus p99.9=%9.1fus max=%10.1fus  samples=%d avg_batch=%.1f allocs/cmd=%.1f gc=%d (%.1fms)\n",
		r.mode, r.workload, r.targetRate, r.completed, r.dropped, r.throughput, r.p50, r.p99, r.p999, r.maxv, r.samples,
		safeDiv(float64(r.commands), float64(r.batches)), r.allocsPerCmd, r.gcCycles, r.gcPauseMs)
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// ---- environment and CSV ----

func sh(name string, args ...string) string {
	b, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func cpuModel() string {
	if v := sh("sysctl", "-n", "machdep.cpu.brand_string"); v != "" {
		return v
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				return strings.TrimSpace(l[strings.Index(l, ":")+1:])
			}
		}
	}
	return "unknown"
}

func memGiB() string {
	if v := sh("sysctl", "-n", "hw.memsize"); v != "" {
		var n float64
		fmt.Sscan(v, &n)
		return fmt.Sprintf("%.0f", n/(1<<30))
	}
	return ""
}

func commit() string {
	if v := sh("git", "rev-parse", "--short", "HEAD"); v != "" {
		return v
	}
	return "none"
}

var header = []string{"run_id", "ts_utc", "mode", "workload", "seed", "symbols", "target_rate", "duration_s", "warmup_s", "completed", "dropped", "rejects",
	"throughput", "p50_us", "p99_us", "p999_us", "max_us", "samples", "avg_batch", "allocs_per_cmd", "gc_cycles", "gc_pause_ms",
	"go", "gomaxprocs", "cpu", "cpus", "mem_gib", "os", "commit", "note"}

func appendCSV(r result) {
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatalf("%v", err)
	}
	_, statErr := os.Stat(*out)
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fatalf("%v", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if statErr != nil {
		w.Write(header)
	}
	w.Write([]string{r.runID, r.ts, r.mode, r.workload, fmt.Sprint(r.seed), fmt.Sprint(r.symbols), fmt.Sprint(r.targetRate), fmt.Sprint(r.dur.Seconds()), fmt.Sprint(r.warm.Seconds()),
		fmt.Sprint(r.completed), fmt.Sprint(r.dropped), fmt.Sprint(r.rejects), fmt.Sprintf("%.0f", r.throughput),
		fmt.Sprintf("%.1f", r.p50), fmt.Sprintf("%.1f", r.p99), fmt.Sprintf("%.1f", r.p999), fmt.Sprintf("%.1f", r.maxv), fmt.Sprint(r.samples),
		fmt.Sprintf("%.2f", safeDiv(float64(r.commands), float64(r.batches))), fmt.Sprintf("%.2f", r.allocsPerCmd), fmt.Sprint(r.gcCycles), fmt.Sprintf("%.2f", r.gcPauseMs),
		runtime.Version(), fmt.Sprint(runtime.GOMAXPROCS(0)), cpuModel(), fmt.Sprint(runtime.NumCPU()), memGiB(), runtime.GOOS + "/" + runtime.GOARCH, commit(), *note})
	w.Flush()
}

// ---- recovery workload ----

func runRecovery() {
	n := *nRecovery
	for _, withSnap := range []bool{false, true} {
		dir, _ := os.MkdirTemp("", "pitbook-recovery-*")
		cfg := engine.Config{Symbol: "R", Dir: dir, WALMode: wal.ModeAsync, QueueSize: 1 << 16, MaxBatch: 1024, KeepAllWAL: true}
		if withSnap {
			cfg.SnapshotEvery = uint64(n / 2)
		}
		e, _, err := engine.Open(cfg)
		if err != nil {
			fatalf("%v", err)
		}
		g := newGen(*seed, false)
		g.preload(e, 5000)
		for i := 0; i < n-5000; i++ {
			p := reqPool.Get().(*reqP)
			p.st, p.due, p.req.Cmd = &symState{measureAt: time.Now().Add(time.Hour)}, time.Now(), g.next()
			for e.Submit(&p.req) != nil {
				runtime.Gosched()
			}
		}
		for e.Stats().LastSeq < uint64(n) {
			time.Sleep(time.Millisecond)
		}
		e.Crash() // no final snapshot: recovery uses what a crash would leave
		start := time.Now()
		e2, rec, err := engine.Open(engine.Config{Symbol: "R", Dir: dir, WALMode: wal.ModeAsync, SnapshotEvery: cfg.SnapshotEvery, KeepAllWAL: true})
		took := time.Since(start)
		if err != nil {
			fatalf("recover: %v", err)
		}
		e2.Close()
		os.RemoveAll(dir)
		label := "no-snapshot"
		if withSnap {
			label = "with-snapshot"
		}
		fmt.Printf("recovery %-13s commands=%d snapshot_seq=%d replayed=%d took=%s\n", label, n, rec.SnapshotSeq, rec.Replayed, took.Round(time.Millisecond))
		appendRecoveryCSV(label, n, rec, took)
	}
}

func appendRecoveryCSV(label string, n int, rec engine.Recovery, took time.Duration) {
	path := strings.TrimSuffix(*out, filepath.Ext(*out)) + "-recovery.csv"
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fatalf("%v", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if statErr != nil {
		w.Write([]string{"ts_utc", "variant", "commands", "snapshot_seq", "replayed", "took_ms", "go", "cpu", "os", "commit", "note"})
	}
	w.Write([]string{time.Now().UTC().Format(time.RFC3339), label, fmt.Sprint(n), fmt.Sprint(rec.SnapshotSeq), fmt.Sprint(rec.Replayed),
		fmt.Sprint(took.Milliseconds()), runtime.Version(), cpuModel(), runtime.GOOS + "/" + runtime.GOARCH, commit(), *note})
	w.Flush()
}

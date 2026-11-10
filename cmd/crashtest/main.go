// Command crashtest is the crash-safety proof (docs/testing.md).
//
// Each run starts a real pitbook process with a synchronous WAL, drives it with
// concurrent HTTP clients that record every response they receive, SIGKILLs it
// at a random moment, restarts it, and then checks, against the log files on
// disk:
//
//  1. every command a client saw acknowledged is in the recovered log at the
//     sequence number the client was told, and is byte-for-byte the command it
//     sent (nothing acknowledged was lost);
//  2. every fill a client saw matches the fill derived by replaying the log,
//     trade ids are unique (nothing was duplicated);
//  3. the restarted server's state hash equals a clean replay from sequence 1
//     (snapshot + log-suffix recovery equals full replay);
//  4. re-sending the commands that were in flight when the process died, with
//     the same client order ids, never creates a second order: a command that
//     reached the log is answered "duplicate", one that did not is accepted.
//
// SIGKILL tests a process crash. It does not test power loss.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/engine"
	"pitbook/internal/gateway"
	"pitbook/internal/wal"
)

type evJSON struct {
	Type          string `json:"type"`
	Seq           uint64 `json:"seq"`
	ClientOrderID uint64 `json:"client_order_id"`
	OrderID       uint64 `json:"order_id"`
	Status        string `json:"status"`
	Code          string `json:"code"`
	Price         int64  `json:"price"`
	Qty           int64  `json:"qty"`
	TradeID       uint64 `json:"trade_id"`
	MakerOrderID  uint64 `json:"maker_order_id"`
	TakerOrderID  uint64 `json:"taker_order_id"`
}

type reply struct {
	Type   string   `json:"type"`
	Code   string   `json:"code"`
	Events []evJSON `json:"events"`
}

// req is one command a client sent and what it learned about it.
type req struct {
	worker int
	body   gateway.ClientMsg
	want   book.Command
	done   bool // a response arrived
	seq    uint64
	events []evJSON
	dup    bool
}

type runResult struct {
	Seed          uint64   `json:"seed"`
	KillAfterMs   int64    `json:"kill_after_ms"`
	SnapshotEvery uint64   `json:"snapshot_every"`
	Acked         int      `json:"acked_commands"`
	InFlight      int      `json:"in_flight_at_kill"`
	FillsChecked  int      `json:"fills_checked"`
	LoggedAfter   uint64   `json:"log_last_seq"`
	RecoveredSnap uint64   `json:"recovered_from_snapshot_seq"`
	Replayed      uint64   `json:"replayed_records"`
	RetriedNew    int      `json:"retried_new_or_replace"`
	RetryDup      int      `json:"retry_answered_duplicate"`
	RetryAccepted int      `json:"retry_accepted_new"`
	UnackedInLog  int      `json:"unacked_but_logged"`
	Failures      []string `json:"failures,omitempty"`
}

var (
	binPath  = flag.String("bin", "bin/pitbook", "pitbook binary")
	runs     = flag.Int("runs", 50, "number of kill/restart runs")
	parallel = flag.Int("parallel", 3, "runs in parallel")
	seed0    = flag.Uint64("seed", 1, "first run seed (run i uses seed+i)")
	workers  = flag.Int("clients", 4, "concurrent clients per run")
	outPath  = flag.String("out", "", "write a JSON report here")
	keepDir  = flag.String("keep-failures", "", "copy data dirs of failing runs here")
	minKill  = flag.Int("min-kill-ms", 40, "earliest kill after the first request, ms")
	maxKill  = flag.Int("max-kill-ms", 900, "latest kill, ms")
)

func main() {
	flag.Parse()
	if _, err := os.Stat(*binPath); err != nil {
		fatalf("binary %s not found (run `make build`): %v", *binPath, err)
	}
	start := time.Now()
	results := make([]runResult, *runs)
	var wg sync.WaitGroup
	sem := make(chan struct{}, *parallel)
	var done atomic.Int32
	for i := 0; i < *runs; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = runOnce(*seed0 + uint64(i))
			n := done.Add(1)
			if len(results[i].Failures) > 0 || n%25 == 0 || int(n) == *runs {
				fmt.Fprintf(os.Stderr, "run %d/%d (seed %d): acked=%d inflight=%d fills=%d failures=%d\n",
					n, *runs, results[i].Seed, results[i].Acked, results[i].InFlight, results[i].FillsChecked, len(results[i].Failures))
			}
		}(i)
	}
	wg.Wait()

	var tot runResult
	failed := 0
	var seeds []uint64
	for _, r := range results {
		seeds = append(seeds, r.Seed)
		tot.Acked += r.Acked
		tot.InFlight += r.InFlight
		tot.FillsChecked += r.FillsChecked
		tot.RetriedNew += r.RetriedNew
		tot.RetryDup += r.RetryDup
		tot.RetryAccepted += r.RetryAccepted
		tot.UnackedInLog += r.UnackedInLog
		if r.RecoveredSnap > 0 {
			tot.RecoveredSnap++
		}
		tot.Replayed += r.Replayed
		if len(r.Failures) > 0 {
			failed++
			for _, f := range r.Failures {
				fmt.Fprintf(os.Stderr, "  seed %d FAIL: %s\n", r.Seed, f)
			}
		}
	}
	fmt.Printf("crashtest: %d runs, %d failed (SIGKILL, sync WAL, %d clients, %s)\n", *runs, failed, *workers, time.Since(start).Round(time.Second))
	fmt.Printf("  acknowledged commands verified in the log: %d (0 lost = %v)\n", tot.Acked, failed == 0)
	fmt.Printf("  fills cross-checked against replay:        %d\n", tot.FillsChecked)
	fmt.Printf("  commands in flight at the kill:            %d (of which %d had reached the log unacknowledged)\n", tot.InFlight, tot.UnackedInLog)
	fmt.Printf("  retried with the same client order id:     %d (answered duplicate %d, accepted new %d)\n", tot.RetriedNew, tot.RetryDup, tot.RetryAccepted)
	fmt.Printf("  runs recovered via snapshot + suffix:      %d of %d\n", tot.RecoveredSnap, *runs)
	if *outPath != "" {
		rep := map[string]any{
			"tool": "cmd/crashtest", "date_utc": time.Now().UTC().Format(time.RFC3339), "runs": *runs, "failed_runs": failed,
			"clients": *workers, "first_seed": *seed0, "seeds": seeds, "kill_window_ms": []int{*minKill, *maxKill},
			"totals": map[string]any{"acked_commands": tot.Acked, "fills_checked": tot.FillsChecked, "in_flight_at_kill": tot.InFlight,
				"unacked_but_logged": tot.UnackedInLog, "retried": tot.RetriedNew, "retry_duplicate": tot.RetryDup, "retry_accepted": tot.RetryAccepted,
				"runs_recovered_via_snapshot": tot.RecoveredSnap},
			"env":     map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": runtime.NumCPU()},
			"results": results,
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*outPath, b, 0o644); err != nil {
			fatalf("write report: %v", err)
		}
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func fatalf(f string, a ...any) { fmt.Fprintf(os.Stderr, f+"\n", a...); os.Exit(2) }

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type server struct {
	cmd  *exec.Cmd
	base string
}

func startServer(dir string, port int, snapEvery uint64, logName string) (*server, error) {
	lf, err := os.OpenFile(filepath.Join(dir, logName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(*binPath, "--listen", fmt.Sprintf("127.0.0.1:%d", port), "--data", filepath.Join(dir, "data"), "--wal", "sync",
		"--keep-all-wal", "--snapshot-every", fmt.Sprint(snapEvery), "--segment-bytes", "65536", "--symbols", "PIT", "--order-rate", "100000")
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s := &server{cmd: cmd, base: fmt.Sprintf("http://127.0.0.1:%d", port)}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return s, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	cmd.Process.Kill()
	return nil, fmt.Errorf("server did not become healthy")
}

func (s *server) kill() { s.cmd.Process.Kill(); s.cmd.Wait() } // SIGKILL

type health struct {
	Status    string            `json:"status"`
	LastSeq   map[string]uint64 `json:"last_seq"`
	StateHash map[string]string `json:"state_hash"`
	Recovery  map[string]struct {
		SnapshotSeq uint64 `json:"snapshot_seq"`
		Replayed    uint64 `json:"replayed"`
	} `json:"recovery"`
}

func getHealth(base string) (health, error) {
	var h health
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		return h, err
	}
	defer resp.Body.Close()
	return h, json.NewDecoder(resp.Body).Decode(&h)
}

func wantCommand(m gateway.ClientMsg) book.Command {
	c := book.Command{Participant: gateway.ParticipantHash(m.ParticipantID), ClientOID: m.ClientOrderID}
	switch m.Type {
	case "NEW":
		c.Kind = book.CmdNew
		c.Side = map[string]book.Side{"buy": book.Buy, "sell": book.Sell}[m.Side]
		c.Type = map[string]book.OrderType{"limit": book.Limit, "market": book.Market, "ioc": book.IOC, "fok": book.FOK}[m.OrderType]
		c.Price, c.Qty = book.Price(m.Price), book.Qty(m.Qty)
	case "CANCEL":
		c.Kind, c.Target = book.CmdCancel, book.OrderID(m.TargetOrderID)
	case "REPLACE":
		c.Kind, c.Target, c.Price, c.Qty = book.CmdReplace, book.OrderID(m.TargetOrderID), book.Price(m.Price), book.Qty(m.Qty)
	}
	return c
}

func post(client *http.Client, base string, m gateway.ClientMsg) (reply, int, error) {
	b, _ := json.Marshal(m)
	resp, err := client.Post(base+"/v1/orders", "application/json", bytes.NewReader(b))
	if err != nil {
		return reply{}, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return reply{}, resp.StatusCode, err
	}
	var r reply
	if err := json.Unmarshal(data, &r); err != nil {
		return r, resp.StatusCode, fmt.Errorf("bad response %q", data)
	}
	return r, resp.StatusCode, nil
}

func runOnce(seed uint64) (res runResult) {
	res.Seed = seed
	dir, err := os.MkdirTemp("", "pitbook-crash-*")
	if err != nil {
		res.Failures = append(res.Failures, err.Error())
		return
	}
	failf := func(f string, a ...any) { res.Failures = append(res.Failures, fmt.Sprintf(f, a...)) }
	defer func() {
		if len(res.Failures) > 0 && *keepDir != "" {
			dst := filepath.Join(*keepDir, fmt.Sprintf("seed-%d", seed))
			os.MkdirAll(*keepDir, 0o755)
			exec.Command("cp", "-R", dir, dst).Run()
		}
		os.RemoveAll(dir)
	}()
	rng := rand.New(rand.NewPCG(seed, 0xc0ffee))
	res.SnapshotEvery = uint64(100 + rng.IntN(500))
	res.KillAfterMs = int64(*minKill + rng.IntN(*maxKill-*minKill+1))
	port := freePort()
	srv, err := startServer(dir, port, res.SnapshotEvery, "server1.log")
	if err != nil {
		failf("start: %v", err)
		return
	}

	// ---- load phase ----
	var mu sync.Mutex
	var all []*req
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	hc := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}}
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wr := rand.New(rand.NewPCG(seed, uint64(w)+1))
			who := fmt.Sprintf("client-%d", w)
			var coid uint64
			var mine []uint64
			for ctx.Err() == nil {
				coid++
				m := gateway.ClientMsg{Symbol: "PIT", ParticipantID: who, ClientOrderID: coid}
				switch r := wr.IntN(100); {
				case r < 72 || len(mine) == 0:
					m.Type = "NEW"
					m.Side = []string{"buy", "sell"}[wr.IntN(2)]
					m.OrderType = "limit"
					switch x := wr.IntN(100); {
					case x > 92:
						m.OrderType = "fok"
					case x > 84:
						m.OrderType = "ioc"
					case x > 78:
						m.OrderType = "market"
					}
					m.Qty = int64(wr.IntN(9) + 1)
					if m.OrderType != "market" {
						m.Price = int64(95 + wr.IntN(11))
					}
				case r < 87:
					m.Type, m.TargetOrderID = "CANCEL", mine[wr.IntN(len(mine))]
				default:
					m.Type, m.TargetOrderID = "REPLACE", mine[wr.IntN(len(mine))]
					m.Price, m.Qty = int64(95+wr.IntN(11)), int64(wr.IntN(9)+1)
				}
				q := &req{worker: w, body: m, want: wantCommand(m)}
				mu.Lock()
				all = append(all, q)
				mu.Unlock()
				r, status, err := post(hc, srv.base, m)
				if err != nil {
					return // connection died with the process: this one is in flight
				}
				if status == 429 || status == 503 {
					continue
				}
				mu.Lock()
				q.done, q.events = true, r.Events
				if len(r.Events) > 0 {
					q.seq = r.Events[0].Seq
					q.dup = r.Events[0].Status == "duplicate"
				}
				mu.Unlock()
				for _, e := range r.Events {
					if e.Type == "ACK" && e.Status == "accepted" && e.OrderID != 0 {
						mine = append(mine, e.OrderID)
					}
				}
			}
		}(w)
	}
	time.Sleep(time.Duration(res.KillAfterMs) * time.Millisecond)
	srv.kill()
	cancel()
	wg.Wait()
	hc.CloseIdleConnections()

	mu.Lock()
	var acked, inflight []*req
	for _, q := range all {
		if q.done && q.seq != 0 {
			acked = append(acked, q)
		} else if !q.done {
			inflight = append(inflight, q)
		}
	}
	mu.Unlock()
	res.Acked, res.InFlight = len(acked), len(inflight)

	// ---- restart and verify ----
	srv2, err := startServer(dir, port, res.SnapshotEvery, "server2.log")
	if err != nil {
		failf("restart after kill: %v", err)
		return
	}
	defer srv2.kill()
	h, err := getHealth(srv2.base)
	if err != nil || h.Status != "ok" {
		failf("healthz after restart: %v %+v", err, h)
		return
	}
	res.RecoveredSnap = h.Recovery["PIT"].SnapshotSeq
	res.Replayed = h.Recovery["PIT"].Replayed
	logCmds, fills, m, err := replayLog(filepath.Join(dir, "data", "PIT"))
	if err != nil {
		failf("full replay from sequence 1: %v", err)
		return
	}
	res.LoggedAfter = m.LastSeq()
	if m.LastSeq() != h.LastSeq["PIT"] || fmt.Sprintf("%016x", m.Hash()) != h.StateHash["PIT"] {
		failf("recovered state (seq %d hash %s) != full replay (seq %d hash %016x)", h.LastSeq["PIT"], h.StateHash["PIT"], m.LastSeq(), m.Hash())
	}
	checkAcked(acked, logCmds, fills, &res, failf)
	if dups := duplicateLogged(logCmds); dups != "" {
		failf("%s", dups)
	}
	ackedSeq := map[uint64]bool{}
	for _, q := range acked {
		ackedSeq[q.seq] = true
	}
	for s := range logCmds {
		if !ackedSeq[s] {
			res.UnackedInLog++ // logged but the client never heard back (killed between fsync and response)
		}
	}

	// ---- retry what was in flight ----
	loggedBefore := map[[3]uint64]uint64{} // (participant, coid, kind) -> seq
	for s, c := range logCmds {
		if c.Kind == book.CmdNew || c.Kind == book.CmdReplace {
			loggedBefore[[3]uint64{uint64(c.Participant), c.ClientOID, uint64(c.Kind)}] = s
		}
	}
	hc2 := &http.Client{Timeout: 20 * time.Second}
	for _, q := range inflight {
		if q.want.Kind == book.CmdCancel {
			continue
		}
		res.RetriedNew++
		r, status, err := post(hc2, srv2.base, q.body)
		if err != nil || len(r.Events) == 0 {
			failf("retry coid %d/%s: %v status %d", q.body.ClientOrderID, q.body.ParticipantID, err, status)
			continue
		}
		first := r.Events[0]
		_, wasLogged := loggedBefore[[3]uint64{uint64(q.want.Participant), q.want.ClientOID, uint64(q.want.Kind)}]
		switch {
		case wasLogged && first.Status == "accepted":
			failf("retry of logged command %s/%d was accepted as new (would be a double order)", q.body.ParticipantID, q.body.ClientOrderID)
		case wasLogged && q.want.Kind == book.CmdNew && first.Type == "ACK" && first.Status != "duplicate":
			failf("retry of logged NEW %s/%d: unexpected %+v", q.body.ParticipantID, q.body.ClientOrderID, first)
		}
		if first.Status == "duplicate" {
			res.RetryDup++
			if !wasLogged {
				failf("retry of NOT logged command %s/%d answered duplicate", q.body.ParticipantID, q.body.ClientOrderID)
			}
			if want := loggedBefore[[3]uint64{uint64(q.want.Participant), q.want.ClientOID, uint64(q.want.Kind)}]; q.want.Kind == book.CmdNew && first.OrderID != want {
				failf("duplicate answer for %s/%d names order %d, log says %d", q.body.ParticipantID, q.body.ClientOrderID, first.OrderID, want)
			}
		} else if first.Status == "accepted" {
			res.RetryAccepted++
		}
	}
	// Final consistency after the retries: live state == full replay, one order per client order id.
	h2, err := getHealth(srv2.base)
	if err != nil {
		failf("healthz after retries: %v", err)
		return
	}
	logCmds2, _, m2, err := replayLog(filepath.Join(dir, "data", "PIT"))
	if err != nil {
		failf("replay after retries: %v", err)
		return
	}
	if m2.LastSeq() != h2.LastSeq["PIT"] || fmt.Sprintf("%016x", m2.Hash()) != h2.StateHash["PIT"] {
		failf("after retries: live state (seq %d %s) != replay (seq %d %016x)", h2.LastSeq["PIT"], h2.StateHash["PIT"], m2.LastSeq(), m2.Hash())
	}
	if dups := duplicateLogged(logCmds2); dups != "" {
		failf("after retries: %s", dups)
	}
	return
}

// replayLog reads the whole log from sequence 1, applying every command to a
// fresh machine, and returns the commands by sequence plus every fill by trade id.
func replayLog(dataDir string) (map[uint64]book.Command, map[uint64]book.Event, *engine.Machine, error) {
	cmds := map[uint64]book.Command{}
	fills := map[uint64]book.Event{}
	m := engine.NewMachine()
	var evs []book.Event
	_, err := wal.Replay(filepath.Join(dataDir, "wal"), 0, false, func(seq uint64, typ byte, body []byte) error {
		c, err := engine.DecodeCommand(body)
		if err != nil {
			return err
		}
		cmds[seq] = c
		evs = m.Apply(seq, c, evs[:0])
		for _, e := range evs {
			if e.Kind == book.EvFill {
				if _, dup := fills[e.TradeID]; dup {
					return fmt.Errorf("trade id %d produced twice", e.TradeID)
				}
				fills[e.TradeID] = e
			}
		}
		return nil
	})
	return cmds, fills, m, err
}

func checkAcked(acked []*req, cmds map[uint64]book.Command, fills map[uint64]book.Event, res *runResult, failf func(string, ...any)) {
	seenTrade := map[uint64]bool{}
	for _, q := range acked {
		got, ok := cmds[q.seq]
		if !ok {
			failf("LOST: client %s coid %d was acknowledged at seq %d but the recovered log ends at %d", q.body.ParticipantID, q.body.ClientOrderID, q.seq, len(cmds))
			continue
		}
		if got != q.want {
			failf("MISMATCH at seq %d: client sent %+v, log has %+v", q.seq, q.want, got)
		}
		for _, e := range q.events {
			switch e.Type {
			case "ACK":
				if q.want.Kind == book.CmdNew && e.OrderID != q.seq {
					failf("seq %d: ack names order %d", q.seq, e.OrderID)
				}
			case "FILL":
				res.FillsChecked++
				f, ok := fills[e.TradeID]
				if !ok {
					failf("PHANTOM FILL: client saw trade %d at seq %d, replay has no such trade", e.TradeID, q.seq)
					continue
				}
				if int64(f.Price) != e.Price || int64(f.Qty) != e.Qty || uint64(f.MakerID) != e.MakerOrderID || uint64(f.TakerID) != e.TakerOrderID || f.Seq != q.seq {
					failf("FILL MISMATCH trade %d: client %+v replay %+v", e.TradeID, e, f)
				}
				if seenTrade[e.TradeID] {
					failf("DUPLICATE FILL: trade %d reported twice to clients", e.TradeID)
				}
				seenTrade[e.TradeID] = true
			}
		}
	}
}

// duplicateLogged reports a (participant, client order id) that was sequenced
// more than once as New or Replace, which idempotency must make impossible.
func duplicateLogged(cmds map[uint64]book.Command) string {
	seen := map[[3]uint64]uint64{}
	seqs := make([]uint64, 0, len(cmds))
	for s := range cmds {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, s := range seqs {
		c := cmds[s]
		if c.Kind != book.CmdNew && c.Kind != book.CmdReplace {
			continue
		}
		k := [3]uint64{uint64(c.Participant), c.ClientOID, uint64(c.Kind)}
		if p, ok := seen[k]; ok {
			return fmt.Sprintf("DOUBLE ORDER: participant %d coid %d logged at seq %d and again at %d", c.Participant, c.ClientOID, p, s)
		}
		seen[k] = s
	}
	return strings.TrimSpace("")
}

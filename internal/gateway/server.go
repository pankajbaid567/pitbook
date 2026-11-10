package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"pitbook/internal/book"
	"pitbook/internal/engine"
	"pitbook/internal/wal"
)

// Config configures a Server.
type Config struct {
	Symbols        []Symbol
	DataDir        string
	NoWAL          bool
	WALMode        wal.Mode
	SnapshotEvery  uint64
	SegmentBytes   int64
	KeepAllWAL     bool
	QueueSize      int
	AllowedOrigins []string // CORS and WebSocket origin patterns, e.g. "https://pitbook.pages.dev"
	Demo           bool     // enables POST /demo/crash
	OrderRate      float64  // REST orders per second per client address (default 30)
	WSRate         float64  // WebSocket messages per second per connection (default 40)
	Version        string
	Logf           func(format string, args ...any)
	Exit           func(code int) // defaults to os.Exit (tests override)
	BeforeCrash    func()         // demo crash: called after the HTTP reply and before Exit (pause bots, final archive sync)
	ArchiveStatus  func() map[string]any
}

// Server wires engines, hubs and HTTP handlers.
type Server struct {
	cfg     Config
	syms    map[string]Symbol
	engines map[string]*engine.Engine
	hubs    map[string]*hub
	recov   map[string]engine.Recovery
	ready   atomic.Bool
	started time.Time
	ipl     *ipLimiter
	crashAt atomic.Int64
	conns   atomic.Int64
	openErr atomic.Value // string
	paused  atomic.Bool
	mu      sync.Mutex
}

// New prepares a server. Call Open to recover state and become ready.
func New(cfg Config) *Server {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	if cfg.Exit == nil {
		cfg.Exit = os.Exit
	}
	if cfg.OrderRate <= 0 {
		cfg.OrderRate = 30
	}
	if cfg.WSRate <= 0 {
		cfg.WSRate = 40
	}
	s := &Server{cfg: cfg, syms: map[string]Symbol{}, engines: map[string]*engine.Engine{}, hubs: map[string]*hub{},
		recov: map[string]engine.Recovery{}, started: time.Now(), ipl: newIPLimiter(cfg.OrderRate, cfg.OrderRate*2)}
	for _, sym := range cfg.Symbols {
		s.syms[sym.Name] = sym
		s.hubs[sym.Name] = newHub(sym)
	}
	return s
}

// Open recovers every symbol's engine. Until it returns nil, /healthz is 503.
func (s *Server) Open() error {
	for _, sym := range s.cfg.Symbols {
		h := s.hubs[sym.Name]
		e, rec, err := engine.Open(engine.Config{
			Symbol: sym.Name, Dir: filepath.Join(s.cfg.DataDir, sym.Name), NoWAL: s.cfg.NoWAL, WALMode: s.cfg.WALMode,
			SegmentBytes: s.cfg.SegmentBytes, SnapshotEvery: s.cfg.SnapshotEvery, KeepAllWAL: s.cfg.KeepAllWAL,
			QueueSize: s.cfg.QueueSize, Detailed: true, OnEvents: h.apply, OnReplay: h.replay,
		})
		if err != nil {
			s.openErr.Store(err.Error())
			return fmt.Errorf("symbol %s: %w", sym.Name, err)
		}
		if err := e.Do(h.seed); err != nil {
			return err
		}
		s.mu.Lock()
		s.engines[sym.Name], s.recov[sym.Name] = e, rec
		s.mu.Unlock()
		s.cfg.Logf("symbol %s recovered: snapshot=%d replayed=%d last_seq=%d hash=%016x in %s",
			sym.Name, rec.SnapshotSeq, rec.Replayed, rec.LastSeq, rec.Hash, rec.Took)
	}
	s.ready.Store(true)
	return nil
}

// Engine returns a symbol's engine (nil before Open completes).
func (s *Server) Engine(sym string) *engine.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.engines[sym]
}

// Paused reports whether the server is refusing new orders (demo crash in progress).
func (s *Server) Paused() bool { return s.paused.Load() }

// Symbols returns the configured symbols.
func (s *Server) Symbols() []Symbol { return s.cfg.Symbols }

// Close shuts every engine down gracefully (drain, final snapshot).
func (s *Server) Close() error {
	s.ready.Store(false)
	var first error
	s.mu.Lock()
	es := make([]*engine.Engine, 0, len(s.engines))
	for _, e := range s.engines {
		es = append(es, e)
	}
	s.mu.Unlock()
	for _, e := range es {
		if err := e.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/symbols", s.guard(s.handleSymbols))
	mux.HandleFunc("GET /v1/stats", s.guard(s.handleStats))
	mux.HandleFunc("GET /v1/book/{symbol}", s.guard(s.handleBook))
	mux.HandleFunc("GET /v1/trades/{symbol}", s.guard(s.handleTrades))
	mux.HandleFunc("POST /v1/orders", s.guard(s.handleOrder))
	mux.HandleFunc("DELETE /v1/orders/{symbol}/{order_id}", s.guard(s.handleCancel))
	mux.HandleFunc("GET /v1/ws", s.handleWS)
	mux.HandleFunc("POST /demo/crash", s.handleCrash)
	mux.HandleFunc("OPTIONS /", s.handlePreflight)
	return mux
}

// ---- helpers ----

func (s *Server) originAllowed(o string) bool {
	if o == "" {
		return false
	}
	for _, a := range s.cfg.AllowedOrigins {
		if a == "*" || strings.EqualFold(a, o) {
			return true
		}
	}
	return false
}

func (s *Server) cors(w http.ResponseWriter, r *http.Request) {
	if o := r.Header.Get("Origin"); s.originAllowed(o) {
		w.Header().Set("Access-Control-Allow-Origin", o)
		w.Header().Set("Vary", "Origin")
	}
}

func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	s.cors(w, r)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Max-Age", "600")
	w.WriteHeader(http.StatusNoContent)
}

// guard adds CORS and refuses requests until recovery has finished.
func (s *Server) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.cors(w, r)
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "recovering"})
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// clientIP uses the rightmost X-Forwarded-For entry (added by the trusted proxy
// in front of us; anything to its left is client-supplied) or the socket peer.
func clientIP(r *http.Request) string {
	if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
		parts := strings.Split(xf, ",")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return h
}

// ---- command execution shared by REST and WebSocket ----

type orderReply struct {
	Type   string      `json:"type"`
	Code   string      `json:"code,omitempty"`
	Msg    string      `json:"message,omitempty"`
	Events []EventJSON `json:"events,omitempty"`
}

// execute validates and runs one command, returning the events addressed to
// its submitter (ack/reject/cancel/fills) and an HTTP status.
func (s *Server) execute(m ClientMsg) (orderReply, int) {
	if s.paused.Load() {
		return orderReply{Type: "REJECT", Code: CodeBusy, Msg: "engine is restarting"}, http.StatusServiceUnavailable
	}
	cmd, _, rj := BuildCommand(m, s.syms)
	if rj != nil {
		return orderReply{Type: "REJECT", Code: rj.Code, Msg: rj.Message}, http.StatusUnprocessableEntity
	}
	e := s.Engine(m.Symbol)
	if e == nil {
		return orderReply{Type: "REJECT", Code: CodeInternal, Msg: "engine not ready"}, http.StatusServiceUnavailable
	}
	res, err := e.Exec(cmd, 5*time.Second)
	switch {
	case errors.Is(err, engine.ErrBusy):
		return orderReply{Type: "REJECT", Code: CodeBusy, Msg: "engine queue is full, retry"}, http.StatusServiceUnavailable
	case err != nil:
		return orderReply{Type: "REJECT", Code: CodeInternal, Msg: err.Error()}, http.StatusInternalServerError
	}
	out := orderReply{Type: "REPLY"}
	status := http.StatusOK
	for _, ev := range res.Events {
		switch ev.Kind {
		case book.EvDelta:
			continue // public feed only
		case book.EvReject:
			status = http.StatusUnprocessableEntity
		}
		out.Events = append(out.Events, ToJSON(ev))
	}
	return out, status
}

// ---- REST ----

func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	if !s.ipl.allow(clientIP(r)) {
		writeJSON(w, http.StatusTooManyRequests, orderReply{Type: "REJECT", Code: CodeRateLimited, Msg: "slow down"})
		return
	}
	var m ClientMsg
	dec := json.NewDecoder(io.LimitReader(r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		writeJSON(w, http.StatusBadRequest, orderReply{Type: "REJECT", Code: CodeBadField, Msg: "invalid JSON: " + err.Error()})
		return
	}
	if m.Type == "" {
		m.Type = "NEW"
	}
	out, status := s.execute(m)
	writeJSON(w, status, out)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if !s.ipl.allow(clientIP(r)) {
		writeJSON(w, http.StatusTooManyRequests, orderReply{Type: "REJECT", Code: CodeRateLimited, Msg: "slow down"})
		return
	}
	id, err := strconv.ParseUint(r.PathValue("order_id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, orderReply{Type: "REJECT", Code: CodeBadField, Msg: "bad order id"})
		return
	}
	coid, _ := strconv.ParseUint(r.URL.Query().Get("client_order_id"), 10, 64)
	out, status := s.execute(ClientMsg{Type: "CANCEL", Symbol: r.PathValue("symbol"), ParticipantID: r.URL.Query().Get("participant_id"),
		ClientOrderID: coid, TargetOrderID: id})
	writeJSON(w, status, out)
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.cfg.Symbols)
}

func (s *Server) handleBook(w http.ResponseWriter, r *http.Request) {
	h, ok := s.hubs[r.PathValue("symbol")]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
	}
	depth, _ := strconv.Atoi(r.URL.Query().Get("depth"))
	writeJSON(w, http.StatusOK, h.Snapshot(depth))
}

func (s *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	h, ok := s.hubs[r.PathValue("symbol")]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	writeJSON(w, http.StatusOK, h.recentTrades(limit))
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.cors(w, r)
	if !s.ready.Load() {
		body := map[string]any{"status": "recovering"}
		if e, ok := s.openErr.Load().(string); ok {
			body["status"], body["error"] = "failed", e
		}
		writeJSON(w, http.StatusServiceUnavailable, body)
		return
	}
	seqs, hashes, recov := map[string]uint64{}, map[string]string{}, map[string]any{}
	for _, sym := range s.cfg.Symbols {
		e := s.Engine(sym.Name)
		var h, q uint64
		done := make(chan struct{})
		go func() { _ = e.Do(func(m *engine.Machine) { h, q = m.Hash(), m.LastSeq() }); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "engine unresponsive", "symbol": sym.Name})
			return
		}
		seqs[sym.Name], hashes[sym.Name] = q, fmt.Sprintf("%016x", h)
		rc := s.recov[sym.Name]
		recov[sym.Name] = map[string]any{"snapshot_seq": rc.SnapshotSeq, "replayed": rc.Replayed, "last_seq": rc.LastSeq,
			"hash": fmt.Sprintf("%016x", rc.Hash), "took_ms": rc.Took.Milliseconds()}
	}
	body := map[string]any{"status": "ok", "version": s.cfg.Version, "last_seq": seqs, "state_hash": hashes,
		"recovery": recov, "uptime_s": int(time.Since(s.started).Seconds())}
	if s.cfg.ArchiveStatus != nil {
		body["archive"] = s.cfg.ArchiveStatus()
	}
	writeJSON(w, http.StatusOK, body)
}

func us(ns int64) float64 { return float64(ns) / 1000 }

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	syms := map[string]any{}
	for _, sym := range s.cfg.Symbols {
		st := s.Engine(sym.Name).Stats()
		syms[sym.Name] = map[string]any{
			"commands": st.Commands, "batches": st.Batches, "duplicates": st.Dups, "busy_rejects": st.Busy,
			"last_seq": st.LastSeq, "queue": st.Queue, "queue_cap": st.QueueCap,
			"clients": s.hubs[sym.Name].numClients(), "dropped_frames": s.hubs[sym.Name].dropped.Load(),
			"latency_us": map[string]any{
				"total_p50": us(st.Total.PercentileNS(50)), "total_p99": us(st.Total.PercentileNS(99)), "total_max": us(st.Total.MaxNS()),
				"sync_p50": us(st.Sync.PercentileNS(50)), "sync_p99": us(st.Sync.PercentileNS(99)),
				"queue_p50": us(st.QueueWait.PercentileNS(50)), "queue_p99": us(st.QueueWait.PercentileNS(99)),
			},
			"avg_batch": st.BatchSize.MeanNS(),
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": s.cfg.Version, "demo": s.cfg.Demo, "uptime_s": int(time.Since(s.started).Seconds()),
		"symbols": syms, "go": map[string]any{"goroutines": runtime.NumGoroutine(), "heap_mb": ms.HeapAlloc >> 20, "gc_cycles": ms.NumGC,
			"gc_pause_total_ms": ms.PauseTotalNs / 1e6, "gomaxprocs": runtime.GOMAXPROCS(0)}, "ws_connections": s.conns.Load()})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if !s.ready.Load() {
		fmt.Fprintln(w, "pitbook_ready 0")
		return
	}
	fmt.Fprintln(w, "pitbook_ready 1")
	fmt.Fprintf(w, "pitbook_uptime_seconds %d\n", int(time.Since(s.started).Seconds()))
	fmt.Fprintf(w, "pitbook_ws_connections %d\n", s.conns.Load())
	for _, sym := range s.cfg.Symbols {
		st := s.Engine(sym.Name).Stats()
		l := fmt.Sprintf(`{symbol=%q}`, sym.Name)
		fmt.Fprintf(w, "pitbook_commands_total%s %d\npitbook_batches_total%s %d\npitbook_duplicates_total%s %d\n", l, st.Commands, l, st.Batches, l, st.Dups)
		fmt.Fprintf(w, "pitbook_busy_rejects_total%s %d\npitbook_last_seq%s %d\npitbook_queue_depth%s %d\n", l, st.Busy, l, st.LastSeq, l, st.Queue)
		fmt.Fprintf(w, "pitbook_ws_clients%s %d\npitbook_ws_dropped_frames_total%s %d\n", l, s.hubs[sym.Name].numClients(), l, s.hubs[sym.Name].dropped.Load())
		for _, q := range []float64{50, 99} {
			ql := fmt.Sprintf(`{symbol=%q,quantile="%g"}`, sym.Name, q/100)
			fmt.Fprintf(w, "pitbook_total_latency_seconds%s %g\npitbook_fsync_seconds%s %g\npitbook_queue_wait_seconds%s %g\n",
				ql, float64(st.Total.PercentileNS(q))/1e9, ql, float64(st.Sync.PercentileNS(q))/1e9, ql, float64(st.QueueWait.PercentileNS(q))/1e9)
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Fprintf(w, "pitbook_go_goroutines %d\npitbook_go_heap_bytes %d\npitbook_go_gc_pause_seconds_total %g\n", runtime.NumGoroutine(), ms.HeapAlloc, float64(ms.PauseTotalNs)/1e9)
}

// handleCrash is the demo-only "kill the engine" button (ADR-013).
func (s *Server) handleCrash(w http.ResponseWriter, r *http.Request) {
	s.cors(w, r)
	if !s.cfg.Demo {
		http.NotFound(w, r)
		return
	}
	now := time.Now().UnixMilli()
	last := s.crashAt.Load()
	if last != 0 && now-last < 60_000 {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "crash button is on cooldown", "retry_after_s": (60_000 - (now - last)) / 1000})
		return
	}
	if !s.crashAt.CompareAndSwap(last, now) {
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "crash already requested"})
		return
	}
	s.cfg.Logf("demo crash requested by %s: exiting with status 137", clientIP(r))
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "crashing", "note": "process exits without flushing; state is rebuilt from the log on restart"})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	go func() {
		s.paused.Store(true) // refuse new orders so nothing is acknowledged after the final sync
		time.Sleep(300 * time.Millisecond)
		if s.cfg.BeforeCrash != nil {
			s.cfg.BeforeCrash()
		}
		s.cfg.Exit(137) // no engine.Close: no final snapshot, no graceful drain
	}()
}

// ---- WebSocket ----

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "recovering", http.StatusServiceUnavailable)
		return
	}
	patterns := make([]string, 0, len(s.cfg.AllowedOrigins))
	for _, o := range s.cfg.AllowedOrigins {
		if o == "*" {
			patterns = append(patterns, "*")
			continue
		}
		patterns = append(patterns, strings.TrimPrefix(strings.TrimPrefix(o, "https://"), "http://"))
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: patterns})
	if err != nil {
		return
	}
	conn.SetReadLimit(4096)
	s.conns.Add(1)
	defer s.conns.Add(-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &client{send: make(chan []byte, 256), priv: make(chan []byte, 16), depth: 20}
	var cur *hub
	defer func() {
		if cur != nil {
			cur.unsubscribe(c)
		}
		conn.CloseNow()
	}()
	go s.wsWriter(ctx, cancel, conn, c)
	lim := newBucket(s.cfg.WSRate, s.cfg.WSRate*2)
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var m ClientMsg
		if err := json.Unmarshal(data, &m); err != nil {
			s.reply(ctx, c, orderReply{Type: "REJECT", Code: CodeBadField, Msg: "invalid JSON"})
			continue
		}
		if m.Type != "PING" && !lim.allow() {
			s.reply(ctx, c, orderReply{Type: "REJECT", Code: CodeRateLimited, Msg: "slow down"})
			continue
		}
		switch m.Type {
		case "PING":
			s.reply(ctx, c, map[string]any{"type": "PONG", "ts_ms": time.Now().UnixMilli()})
		case "SUBSCRIBE", "SNAPSHOT_REQUEST":
			h, ok := s.hubs[m.Symbol]
			if !ok {
				if cur == nil || m.Type == "SUBSCRIBE" {
					s.reply(ctx, c, orderReply{Type: "REJECT", Code: CodeUnknownSymbol, Msg: "unknown symbol"})
					continue
				}
				h = cur
			}
			if cur != nil && cur != h {
				cur.unsubscribe(c)
			}
			if m.Depth > 0 && m.Depth <= maxDepth {
				c.depth = m.Depth
			}
			if cur == h {
				h.resync(c, "REQUESTED")
			} else {
				h.subscribe(c)
			}
			cur = h
		case "NEW", "CANCEL", "REPLACE":
			out, _ := s.execute(m)
			s.reply(ctx, c, out)
		default:
			s.reply(ctx, c, orderReply{Type: "REJECT", Code: CodeBadField, Msg: "unknown message type"})
		}
	}
}

func (s *Server) reply(ctx context.Context, c *client, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.priv <- data:
	case <-ctx.Done():
	}
}

func (s *Server) wsWriter(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, c *client) {
	defer cancel()
	write := func(b []byte) bool {
		wctx, wc := context.WithTimeout(ctx, 5*time.Second)
		defer wc()
		return conn.Write(wctx, websocket.MessageText, b) == nil
	}
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-c.priv:
			if !write(b) {
				return
			}
		case b := <-c.send:
			if !write(b) {
				return
			}
			if c.resync.Load() && c.hub != nil {
				c.hub.resync(c, "SLOW_CONSUMER")
			}
		}
	}
}

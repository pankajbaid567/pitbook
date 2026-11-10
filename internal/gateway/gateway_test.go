package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"pitbook/internal/book"
)

var testSyms = []Symbol{{Name: "PIT", Tick: 1, Lot: 1, PriceScale: 100}, {Name: "ODD", Tick: 5, Lot: 10, PriceScale: 100}}

func newTestServer(t *testing.T, mod func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := Config{Symbols: testSyms, NoWAL: true, AllowedOrigins: []string{"*"}, Version: "test", OrderRate: 10000, WSRate: 10000}
	if mod != nil {
		mod(&cfg)
	}
	s := New(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(func() { ts.Close(); s.Close() })
	return s, ts
}

func post(t *testing.T, ts *httptest.Server, body string) (int, orderReply) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/orders", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r orderReply
	json.NewDecoder(resp.Body).Decode(&r)
	return resp.StatusCode, r
}

func order(sym, who string, coid int, side, typ string, price, qty int) string {
	return fmt.Sprintf(`{"type":"NEW","symbol":%q,"participant_id":%q,"client_order_id":%d,"side":%q,"order_type":%q,"price":%d,"qty":%d}`,
		sym, who, coid, side, typ, price, qty)
}

func getJSON(t *testing.T, url string, v any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if v != nil {
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatalf("%s: %v: %s", url, err, b)
		}
	}
	return resp.StatusCode
}

func TestRESTOrderFlowBookAndTrades(t *testing.T) {
	_, ts := newTestServer(t, nil)
	code, r := post(t, ts, order("PIT", "alice", 1, "sell", "limit", 10000, 10))
	if code != 200 || len(r.Events) != 1 || r.Events[0].Type != "ACK" || r.Events[0].Status != "accepted" {
		t.Fatalf("sell: %d %+v", code, r)
	}
	code, r = post(t, ts, order("PIT", "bob", 1, "buy", "limit", 10000, 4))
	if code != 200 || len(r.Events) != 2 || r.Events[1].Type != "FILL" || r.Events[1].Qty != 4 || r.Events[1].Price != 10000 {
		t.Fatalf("buy: %d %+v", code, r)
	}
	var snap Snapshot
	getJSON(t, ts.URL+"/v1/book/PIT?depth=5", &snap)
	if len(snap.Asks) != 1 || snap.Asks[0] != [2]int64{10000, 6} || len(snap.Bids) != 0 || snap.LastSeq != 2 {
		t.Fatalf("book: %+v", snap)
	}
	var trades []Trade
	getJSON(t, ts.URL+"/v1/trades/PIT", &trades)
	if len(trades) != 1 || trades[0].Qty != 4 || trades[0].TakerSide != "buy" {
		t.Fatalf("trades: %+v", trades)
	}
}

func TestValidationRejectsConsumeNoSequence(t *testing.T) {
	_, ts := newTestServer(t, nil)
	bad := []string{
		order("NOPE", "a", 1, "buy", "limit", 100, 1),
		order("PIT", "", 2, "buy", "limit", 100, 1),
		order("PIT", "a", 3, "up", "limit", 100, 1),
		order("PIT", "a", 4, "buy", "stop", 100, 1),
		order("PIT", "a", 5, "buy", "limit", 0, 1),
		order("PIT", "a", 6, "buy", "limit", 100, 0),
		order("PIT", "a", 7, "buy", "market", 100, 1),
		order("ODD", "a", 8, "buy", "limit", 101, 10), // tick 5
		order("ODD", "a", 9, "buy", "limit", 100, 15), // lot 10
		order("PIT", "a", 10, "buy", "limit", MaxPrice+1, 1),
		order("PIT", "a", 11, "buy", "limit", 100, MaxQty+1),
	}
	wantCodes := []string{CodeUnknownSymbol, CodeBadField, CodeBadField, CodeBadField, CodeBadField, CodeBadField, CodeBadField, CodeNotTick, CodeNotLot, CodeBadField, CodeBadField}
	for i, b := range bad {
		code, r := post(t, ts, b)
		if code != 422 || r.Code != wantCodes[i] {
			t.Fatalf("case %d: status %d reply %+v, want %s", i, code, r, wantCodes[i])
		}
	}
	var h map[string]any
	getJSON(t, ts.URL+"/healthz", &h)
	if h["last_seq"].(map[string]any)["PIT"].(float64) != 0 {
		t.Fatalf("rejected orders consumed sequence numbers: %v", h["last_seq"])
	}
	// Unknown JSON fields and garbage are refused too.
	if code, _ := post(t, ts, `{"type":"NEW","bogus":1}`); code != 400 {
		t.Fatalf("unknown field: %d", code)
	}
	if code, _ := post(t, ts, `not json`); code != 400 {
		t.Fatalf("garbage: %d", code)
	}
}

func TestIdempotentRetryOverREST(t *testing.T) {
	_, ts := newTestServer(t, nil)
	b := order("PIT", "alice", 77, "buy", "limit", 9900, 5)
	_, first := post(t, ts, b)
	_, again := post(t, ts, b)
	if again.Events[0].Status != "duplicate" || again.Events[0].OrderID != first.Events[0].OrderID {
		t.Fatalf("first %+v again %+v", first, again)
	}
	var snap Snapshot
	getJSON(t, ts.URL+"/v1/book/PIT", &snap)
	if len(snap.Bids) != 1 || snap.Bids[0][1] != 5 {
		t.Fatalf("duplicate created a second order: %+v", snap.Bids)
	}
}

func TestCancelOverREST(t *testing.T) {
	_, ts := newTestServer(t, nil)
	_, r := post(t, ts, order("PIT", "alice", 1, "buy", "limit", 9900, 5))
	id := r.Events[0].OrderID
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/v1/orders/PIT/%d?participant_id=alice&client_order_id=2", ts.URL, id), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("cancel: %v %v", resp, err)
	}
	var out orderReply
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Events) != 1 || out.Events[0].Type != "CANCELED" || out.Events[0].Reason != "USER" || out.Events[0].RemainingQty != 5 {
		t.Fatalf("%+v", out)
	}
	// Someone else cannot cancel it, and it is gone for its owner.
	req, _ = http.NewRequest("DELETE", fmt.Sprintf("%s/v1/orders/PIT/%d?participant_id=mallory&client_order_id=3", ts.URL, id), nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 422 {
		t.Fatalf("cancel of someone else's order: %d", resp.StatusCode)
	}
}

func TestHealthzIs503UntilOpen(t *testing.T) {
	s := New(Config{Symbols: testSyms, NoWAL: true})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	if code := getJSON(t, ts.URL+"/healthz", nil); code != 503 {
		t.Fatalf("before open: %d", code)
	}
	if code := getJSON(t, ts.URL+"/v1/symbols", nil); code != 503 {
		t.Fatalf("symbols before open: %d", code)
	}
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if code := getJSON(t, ts.URL+"/healthz", nil); code != 200 {
		t.Fatalf("after open: %d", code)
	}
}

func TestRESTRateLimit(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) { c.OrderRate = 2 }) // burst 4
	limited := 0
	for i := 0; i < 12; i++ {
		if code, _ := post(t, ts, order("PIT", "a", i+1, "buy", "limit", 100, 1)); code == 429 {
			limited++
		}
	}
	if limited < 6 {
		t.Fatalf("expected most of 12 rapid orders to be limited, got %d", limited)
	}
}

func TestCrashEndpoint(t *testing.T) {
	var code atomic.Int32
	_, off := newTestServer(t, nil)
	resp, _ := http.Post(off.URL+"/demo/crash", "", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("crash must be 404 without --demo, got %d", resp.StatusCode)
	}
	_, on := newTestServer(t, func(c *Config) { c.Demo = true; c.Exit = func(n int) { code.Store(int32(n)) } })
	resp, _ = http.Post(on.URL+"/demo/crash", "", nil)
	if resp.StatusCode != 202 {
		t.Fatalf("crash: %d", resp.StatusCode)
	}
	resp, _ = http.Post(on.URL+"/demo/crash", "", nil)
	if resp.StatusCode != 429 {
		t.Fatalf("second crash within cooldown: %d", resp.StatusCode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for code.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if code.Load() != 137 {
		t.Fatalf("exit code %d, want 137", code.Load())
	}
}

func TestCORSAndPreflight(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) { c.AllowedOrigins = []string{"https://pitbook.example"} })
	req, _ := http.NewRequest("OPTIONS", ts.URL+"/v1/orders", nil)
	req.Header.Set("Origin", "https://pitbook.example")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "https://pitbook.example" {
		t.Fatalf("preflight: %d %v", resp.StatusCode, resp.Header)
	}
	req, _ = http.NewRequest("GET", ts.URL+"/v1/symbols", nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, _ = http.DefaultClient.Do(req)
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin got CORS headers")
	}
}

// ---- WebSocket ----

type wsc struct {
	t    *testing.T
	conn *websocket.Conn
	ctx  context.Context
}

func dialWS(t *testing.T, ts *httptest.Server) *wsc {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/v1/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return &wsc{t: t, conn: conn, ctx: ctx}
}

func (w *wsc) send(v any) {
	b, _ := json.Marshal(v)
	if err := w.conn.Write(w.ctx, websocket.MessageText, b); err != nil {
		w.t.Fatal(err)
	}
}

func (w *wsc) recv() map[string]any {
	_, b, err := w.conn.Read(w.ctx)
	if err != nil {
		w.t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		w.t.Fatalf("%v: %s", err, b)
	}
	return m
}

func TestWebSocketSnapshotThenContiguousFramesMatchServerBook(t *testing.T) {
	_, ts := newTestServer(t, nil)
	for i := 0; i < 20; i++ { // pre-existing state before the subscriber joins
		post(t, ts, order("PIT", "mm", i+1, "sell", "limit", 10100+i, 5))
	}
	sub := dialWS(t, ts)
	sub.send(map[string]any{"type": "SUBSCRIBE", "symbol": "PIT", "depth": 200})
	snap := sub.recv()
	if snap["type"] != "SNAPSHOT" || snap["last_seq"].(float64) != 20 {
		t.Fatalf("first message must be the snapshot: %v", snap)
	}
	bids, asks := map[int64]int64{}, map[int64]int64{}
	for _, l := range snap["asks"].([]any) {
		p := l.([]any)
		asks[int64(p[0].(float64))] = int64(p[1].(float64))
	}
	last := uint64(snap["last_seq"].(float64))

	// Traffic from another connection (WebSocket order entry): crossing, cancels, replaces.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tr := dialWS(t, ts)
		for i := 0; i < 150; i++ {
			side := []string{"buy", "sell"}[i%2]
			px := 10090 + (i*7)%25
			tr.send(map[string]any{"type": "NEW", "symbol": "PIT", "participant_id": "trader", "client_order_id": 1000 + i,
				"side": side, "order_type": "limit", "price": px, "qty": 1 + i%6})
			rep := tr.recv()
			if rep["type"] != "REPLY" {
				t.Errorf("reply: %v", rep)
				return
			}
		}
	}()
	wg.Wait()

	target := uint64(0)
	var snapNow Snapshot
	getJSON(t, ts.URL+"/v1/book/PIT?depth=200", &snapNow)
	target = snapNow.LastSeq
	frames := 0
	for last < target {
		f := sub.recv()
		if f["type"] != "EVENTS" {
			t.Fatalf("unexpected %v", f)
		}
		prev, lastSeq := uint64(f["prev_seq"].(float64)), uint64(f["last_seq"].(float64))
		if prev != last {
			t.Fatalf("gap: frame prev_seq=%d but last applied=%d", prev, last)
		}
		last = lastSeq
		frames++
		for _, e := range f["events"].([]any) {
			ev := e.(map[string]any)
			if ev["type"] != "BOOK_DELTA" {
				continue
			}
			m := bids
			if ev["side"] == "sell" {
				m = asks
			}
			p, q := int64(ev["price"].(float64)), int64(ev["qty"].(float64))
			if q == 0 {
				delete(m, p)
			} else {
				m[p] = q
			}
		}
	}
	if frames == 0 {
		t.Fatal("no frames received")
	}
	for _, side := range []struct {
		name string
		got  map[int64]int64
		want [][2]int64
	}{{"bids", bids, snapNow.Bids}, {"asks", asks, snapNow.Asks}} {
		if len(side.got) != len(side.want) {
			t.Fatalf("%s: client view has %d levels, server %d", side.name, len(side.got), len(side.want))
		}
		for _, l := range side.want {
			if side.got[l[0]] != l[1] {
				t.Fatalf("%s level %d: client %d server %d", side.name, l[0], side.got[l[0]], l[1])
			}
		}
	}
	t.Logf("client view rebuilt from snapshot + %d frames equals the server book", frames)
}

func TestWebSocketRejectsBadInputAndHandlesPing(t *testing.T) {
	_, ts := newTestServer(t, nil)
	c := dialWS(t, ts)
	c.send(map[string]any{"type": "PING"})
	if m := c.recv(); m["type"] != "PONG" {
		t.Fatalf("%v", m)
	}
	c.conn.Write(c.ctx, websocket.MessageText, []byte("{{{"))
	if m := c.recv(); m["code"] != CodeBadField {
		t.Fatalf("%v", m)
	}
	c.send(map[string]any{"type": "SUBSCRIBE", "symbol": "NOPE"})
	if m := c.recv(); m["code"] != CodeUnknownSymbol {
		t.Fatalf("%v", m)
	}
	c.send(map[string]any{"type": "WHAT"})
	if m := c.recv(); m["code"] != CodeBadField {
		t.Fatalf("%v", m)
	}
}

func TestWebSocketOriginCheck(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) { c.AllowedOrigins = []string{"https://ok.example"} })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/ws"
	_, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}}})
	if err == nil {
		t.Fatal("foreign origin accepted")
	}
	conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{"https://ok.example"}}})
	if err != nil {
		t.Fatalf("allowed origin refused: %v", err)
	}
	conn.CloseNow()
}

// A subscriber that never drains its queue must be flagged, dropped from the
// live stream, and then resynced with RESYNC + a snapshot that is current, while
// the producer (the engine goroutine) is never blocked.
func TestSlowConsumerIsFlaggedAndResynced(t *testing.T) {
	h := newHub(testSyms[0])
	c := &client{send: make(chan []byte, 4), priv: make(chan []byte, 1), depth: 10}
	h.subscribe(c)
	start := time.Now()
	for i := 1; i <= 100; i++ {
		h.apply([]book.Event{{Kind: book.EvDelta, Side: book.Buy, Price: book.Price(100 + i), Qty: 1, Seq: uint64(i)}})
	}
	if time.Since(start) > time.Second {
		t.Fatal("producer was slowed by a stalled consumer")
	}
	if !c.resync.Load() || h.dropped.Load() == 0 {
		t.Fatal("slow consumer not flagged")
	}
	h.resync(c, "SLOW_CONSUMER")
	if c.resync.Load() {
		t.Fatal("flag not cleared")
	}
	first, second := <-c.send, <-c.send
	var note map[string]any
	json.Unmarshal(first, &note)
	var snap Snapshot
	json.Unmarshal(second, &snap)
	if note["type"] != "RESYNC" || note["reason"] != "SLOW_CONSUMER" || snap.Type != "SNAPSHOT" || snap.LastSeq != 100 || len(snap.Bids) != 10 {
		t.Fatalf("note=%v snap=%+v", note, snap)
	}
	if len(c.send) != 0 {
		t.Fatal("stale frames survived the resync")
	}
	// New frames continue from the snapshot's sequence.
	h.apply([]book.Event{{Kind: book.EvDelta, Side: book.Sell, Price: 500, Qty: 1, Seq: 101}})
	var f Frame
	json.Unmarshal(<-c.send, &f)
	if f.PrevSeq != 100 || f.LastSeq != 101 {
		t.Fatalf("frame after resync: %+v", f)
	}
}

func TestStatsAndMetricsEndpoints(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) {})
	post(t, ts, order("PIT", "a", 1, "buy", "limit", 100, 1))
	var st map[string]any
	getJSON(t, ts.URL+"/v1/stats", &st)
	pit := st["symbols"].(map[string]any)["PIT"].(map[string]any)
	if pit["commands"].(float64) != 1 {
		t.Fatalf("%v", pit)
	}
	resp, _ := http.Get(ts.URL + "/metrics")
	b, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(b, []byte(`pitbook_commands_total{symbol="PIT"} 1`)) || !bytes.Contains(b, []byte("pitbook_ready 1")) {
		t.Fatalf("metrics:\n%s", b)
	}
}

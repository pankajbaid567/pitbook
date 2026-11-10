// Command pitbook runs the matching engines and the HTTP/WebSocket gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"pitbook/internal/archive"
	"pitbook/internal/book"
	"pitbook/internal/bots"
	"pitbook/internal/gateway"
	"pitbook/internal/wal"
)

var version = "dev"

func parseSymbols(s string) ([]gateway.Symbol, error) {
	var out []gateway.Symbol
	for _, part := range strings.Split(s, ",") {
		f := strings.Split(strings.TrimSpace(part), ":")
		sym := gateway.Symbol{Name: f[0], Tick: 1, Lot: 1, PriceScale: 100}
		if sym.Name == "" || len(sym.Name) > 12 {
			return nil, fmt.Errorf("bad symbol %q", part)
		}
		for i, dst := range []*int64{&sym.Tick, &sym.Lot, &sym.PriceScale} {
			if len(f) > i+1 {
				v, err := strconv.ParseInt(f[i+1], 10, 64)
				if err != nil || v <= 0 {
					return nil, fmt.Errorf("bad symbol %q: %q is not a positive integer", part, f[i+1])
				}
				*dst = v
			}
		}
		out = append(out, sym)
	}
	return out, nil
}

func main() {
	var (
		listen    = flag.String("listen", "", "address to listen on (default :$PORT or :8080)")
		dataDir   = flag.String("data", "./data", "data directory (WAL and snapshots)")
		symbols   = flag.String("symbols", "PIT", "comma list of NAME[:tick[:lot[:price_scale]]]")
		walMode   = flag.String("wal", "sync", "sync (ack after fsync) | async (benchmarks only) | off (no log, no durability)")
		snapEvery = flag.Uint64("snapshot-every", 50000, "commands between snapshots (0 disables)")
		segBytes  = flag.Int64("segment-bytes", 64<<20, "WAL segment size")
		keepAll   = flag.Bool("keep-all-wal", false, "never delete WAL segments (lets tools replay from sequence 1)")
		queue     = flag.Int("queue", 4096, "per-symbol command queue capacity")
		demo      = flag.Bool("demo", false, "run demo bots and enable POST /demo/crash")
		botRate   = flag.Float64("bot-rate", 20, "demo bot actions per second per symbol")
		seed      = flag.Uint64("seed", 1, "demo bot seed")
		origins   = flag.String("allowed-origins", os.Getenv("ALLOWED_ORIGIN"), "comma list of origins allowed for CORS and WebSocket (* for any)")
		orderRate = flag.Float64("order-rate", 30, "REST orders per second per client address")
		wsRate    = flag.Float64("ws-rate", 40, "WebSocket messages per second per connection")
		archURL   = flag.String("archive-url", os.Getenv("ARCHIVE_URL"), "store Worker base URL: mirror the WAL there and restore from it on a fresh disk (token in $ARCHIVE_TOKEN)")
		archEvery = flag.Duration("archive-interval", 10*time.Second, "how often to mirror new log data")
		showVer   = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVer {
		fmt.Println(version)
		return
	}
	addr := *listen
	if addr == "" {
		if p := os.Getenv("PORT"); p != "" {
			addr = ":" + p
		} else {
			addr = ":8080"
		}
	}
	syms, err := parseSymbols(*symbols)
	if err != nil {
		log.Fatal(err)
	}
	cfg := gateway.Config{Symbols: syms, DataDir: *dataDir, SnapshotEvery: *snapEvery, SegmentBytes: *segBytes, KeepAllWAL: *keepAll,
		QueueSize: *queue, Demo: *demo, OrderRate: *orderRate, WSRate: *wsRate, Version: version, Logf: log.Printf}
	switch *walMode {
	case "sync":
		cfg.WALMode = wal.ModeSync
	case "async":
		cfg.WALMode = wal.ModeAsync
	case "off":
		cfg.NoWAL = true
	default:
		log.Fatalf("unknown --wal %q", *walMode)
	}
	for _, o := range strings.Split(*origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			cfg.AllowedOrigins = append(cfg.AllowedOrigins, o)
		}
	}
	var store archive.Store
	var mirrors []*archive.Mirror
	var mirrorsMu sync.Mutex
	var restoredFiles atomic.Int64
	var botsPaused atomic.Bool
	if *archURL != "" {
		tok := os.Getenv("ARCHIVE_TOKEN")
		if tok == "" {
			log.Fatal("--archive-url needs ARCHIVE_TOKEN in the environment")
		}
		store = archive.NewHTTPStore(*archURL, tok)
		// A fresh container has an empty disk: pull the log back before the engine
		// opens. If the bucket cannot be read we stop instead of starting empty,
		// because an empty start would then be mirrored over the real data.
		for _, sym := range syms {
			var n int
			var err error
			for attempt := 1; attempt <= 4; attempt++ {
				rctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				n, err = archive.Restore(rctx, store, sym.Name+"/", filepath.Join(*dataDir, sym.Name))
				cancel()
				if err == nil {
					break
				}
				log.Printf("archive restore %s attempt %d failed: %v", sym.Name, attempt, err)
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
			}
			if err != nil {
				log.Fatalf("FATAL: cannot restore %s from the archive: %v", sym.Name, err)
			}
			restoredFiles.Add(int64(n))
			if n > 0 {
				log.Printf("archive: restored %d files for %s", n, sym.Name)
			}
		}
		cfg.ArchiveStatus = func() map[string]any {
			mirrorsMu.Lock()
			defer mirrorsMu.Unlock()
			var up, del int
			var lastOK time.Time
			var errS string
			for _, m := range mirrors {
				u, d, ok, e := m.Status()
				up, del = up+u, del+d
				if ok.After(lastOK) {
					lastOK = ok
				}
				if e != nil {
					errS = e.Error()
				}
			}
			out := map[string]any{"uploads": up, "deletes": del, "restored_files": restoredFiles.Load(), "interval_s": archEvery.Seconds()}
			if !lastOK.IsZero() {
				out["last_ok_age_s"] = int(time.Since(lastOK).Seconds())
			}
			if errS != "" {
				out["error"] = errS
			}
			return out
		}
		cfg.BeforeCrash = func() {
			botsPaused.Store(true)
			time.Sleep(250 * time.Millisecond) // let in-flight commands finish; the server already refuses new orders
			mirrorsMu.Lock()
			ms := append([]*archive.Mirror(nil), mirrors...)
			mirrorsMu.Unlock()
			for _, m := range ms {
				cctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				if err := m.SyncOnce(cctx); err != nil {
					log.Printf("pre-crash archive sync failed: %v", err)
				}
				cancel()
			}
		}
	}
	srv := gateway.New(cfg)
	httpSrv := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 120 * time.Second}

	// Listen first so the platform's health check gets an answer (503 while
	// recovering) instead of a refused connection.
	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Printf("pitbook %s listening on %s (wal=%s data=%s symbols=%s demo=%v)", version, addr, *walMode, *dataDir, *symbols, *demo)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	botsCtx, stopBots := context.WithCancel(ctx)
	defer stopBots()

	go func() {
		if err := srv.Open(); err != nil {
			log.Printf("FATAL: recovery failed: %v", err)
			errc <- err
			return
		}
		log.Printf("ready")
		if store != nil {
			for _, sym := range syms {
				m := archive.NewMirror(store, sym.Name+"/", filepath.Join(*dataDir, sym.Name), *archEvery, log.Printf)
				mirrorsMu.Lock()
				mirrors = append(mirrors, m)
				mirrorsMu.Unlock()
				go m.Run(botsCtx)
			}
		}
		if *demo {
			base := uint64(time.Now().UnixMicro()) // never collides with ids used before a restart
			for i, sym := range syms {
				ex := bots.EngineExec(srv.Engine(sym.Name))
				m := bots.New(bots.Config{Seed: *seed + uint64(i), Mid: 10000, ClientOIDBase: base + uint64(i)<<40, PartID: gateway.ParticipantHash},
					func(c book.Command) ([]book.Event, error) {
						if botsPaused.Load() {
							return nil, errors.New("paused")
						}
						return ex(c)
					})
				go m.Run(botsCtx, *botRate)
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("signal received, shutting down")
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
			os.Exit(1)
		}
	}
	stopBots()
	shCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	if err := srv.Close(); err != nil {
		log.Printf("close: %v", err)
		os.Exit(1)
	}
	log.Printf("clean shutdown")
}

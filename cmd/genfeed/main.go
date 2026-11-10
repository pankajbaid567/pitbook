// Command genfeed records a deterministic market session from the real engine
// and bots into a JSON file the web UI replays while the live engine is asleep.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"pitbook/internal/bots"
	"pitbook/internal/engine"
	"pitbook/internal/gateway"
)

func main() {
	out := flag.String("out", "web/public/feed.json", "output file")
	steps := flag.Int("steps", 1400, "bot steps to record")
	warm := flag.Int("warmup", 3000, "bot steps before recording starts")
	seed := flag.Uint64("seed", 42, "bot seed")
	stepMs := flag.Int64("step-ms", 60, "recorded milliseconds per engine batch")
	flag.Parse()

	sym := gateway.Symbol{Name: "PIT", Tick: 1, Lot: 1, PriceScale: 100}
	rec := gateway.NewRecorder(sym, 1_700_000_000_000, *stepMs)
	e, _, err := engine.Open(engine.Config{Symbol: "PIT", NoWAL: true, OnEvents: rec.OnEvents})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer e.Close()
	m := bots.New(bots.Config{Seed: *seed, Mid: 10000, ClientOIDBase: 1, PartID: gateway.ParticipantHash}, bots.EngineExec(e))
	for i := 0; i < *warm; i++ {
		m.Step()
	}
	rec.Reset()
	snap := rec.Snapshot(40)
	for i := 0; i < *steps; i++ {
		m.Step()
	}
	b, err := json.Marshal(map[string]any{"snapshot": snap, "frames": rec.Frames})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: snapshot at seq %d, %d frames, %d bytes\n", *out, snap.LastSeq, len(rec.Frames), len(b))
}

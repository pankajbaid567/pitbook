// Command replay rebuilds the state of a Pitbook data directory by replaying
// its write-ahead log, without opening it for writing or changing any file, and
// prints the final sequence number, the trade count and the state hash.
//
// Replaying the same directory twice must print the same hash (determinism),
// and the hash must equal the one /healthz reports for a server that recovered
// from the same directory (snapshot + suffix equals full replay).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"pitbook/internal/book"
	"pitbook/internal/engine"
)

func main() {
	dir := flag.String("data", "./data", "data directory containing one subdirectory per symbol")
	symbol := flag.String("symbol", "PIT", "symbol subdirectory to replay")
	fromSnap := flag.Bool("from-snapshot", false, "start from the newest valid snapshot instead of sequence 1")
	torn := flag.Bool("tolerate-torn-tail", true, "ignore a damaged last record (a crashed process may leave one)")
	runs := flag.Int("runs", 1, "replay this many times and require identical hashes")
	flag.Parse()

	path := filepath.Join(*dir, *symbol)
	var first uint64
	for i := 0; i < *runs; i++ {
		var fills, rejects, events uint64
		m, err := engine.ReplayDir(path, *fromSnap, *torn, func(evs []book.Event) {
			for _, e := range evs {
				events++
				switch e.Kind {
				case book.EvFill:
					fills++
				case book.EvReject:
					rejects++
				}
			}
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay %s: %v\n", path, err)
			os.Exit(1)
		}
		h := m.Hash()
		if i == 0 {
			first = h
			fmt.Printf("symbol=%s last_seq=%d resting_orders=%d trades=%d events_since_start=%d fills_since_start=%d rejects_since_start=%d state_hash=%016x\n",
				*symbol, m.LastSeq(), m.Book().Len(), m.Book().Trades(), events, fills, rejects, h)
		} else if h != first {
			fmt.Fprintf(os.Stderr, "NONDETERMINISM: replay %d gave %016x, first gave %016x\n", i+1, h, first)
			os.Exit(1)
		}
	}
	if *runs > 1 {
		fmt.Printf("%d replays, identical state hash\n", *runs)
	}
}

.PHONY: build test race vet fmt lint fuzz prop mutants bench crash-test docs demo web feed clean

build:
	go build -o bin/pitbook ./cmd/pitbook
	go build -o bin/loadgen ./cmd/loadgen
	go build -o bin/replay ./cmd/replay
	go build -o bin/crashtest ./cmd/crashtest
	go build -o bin/genfeed ./cmd/genfeed

vet:
	go vet ./...

fmt:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "gofmt needed" && exit 1)

lint: vet fmt

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

# Full reference-model proof: 1,000,000 random commands (about 90 s).
prop:
	PITBOOK_PROP_FULL=1 go test -count=1 -v -run TestPropertyEngineEqualsReference ./internal/book

# Injects deliberate bugs into a scratch copy of the book; the property test must catch each one.
mutants:
	./scripts/mutation_check.sh

fuzz:
	go test -run xxx -fuzz FuzzDecodeCommand -fuzztime 30s ./internal/engine
	go test -run xxx -fuzz FuzzOpenNeverPanics -fuzztime 30s ./internal/wal

# Full benchmark matrix (about 25 minutes; run on an idle machine on AC power). Raw CSV goes to bench/results.
bench: build
	LOADGEN=bin/loadgen ./scripts/run_bench.sh

# 1,000 SIGKILL runs against the real binary (about 3 minutes). Report goes to bench/results.
crash-test: build
	./bin/crashtest -bin bin/pitbook -runs 1000 -parallel 4 -out bench/results/crashtest-$$(date -u +%Y%m%dT%H%M%SZ).json

# Regenerate BENCHMARKS.md and the UI's proof.json from the raw result files.
docs:
	python3 scripts/make_benchmarks.py > BENCHMARKS.md
	python3 scripts/make_proof.py

# Record the market session the UI plays while a sleeping host wakes up.
feed:
	go run ./cmd/genfeed -out web/public/feed.json

# Local demo: engine with bots on :8080 and the UI dev server on :5173.
demo: build
	./bin/pitbook --demo --data ./data --listen 127.0.0.1:8080 --allowed-origins '*' & \
	cd web && VITE_API_URL=http://127.0.0.1:8080 npm run dev

web:
	cd web && npm install && npm run build

clean:
	rm -rf bin web/dist

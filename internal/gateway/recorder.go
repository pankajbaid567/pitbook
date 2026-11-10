package gateway

import "pitbook/internal/book"

// Recorder turns engine events into the same frames the live feed sends, with a
// deterministic clock, so cmd/genfeed can produce the recorded session the UI
// plays while a sleeping free-tier instance wakes up.
type Recorder struct {
	h      *hub
	Frames []Frame
	now    int64
	step   int64
}

// NewRecorder starts a clock at t0 (ms) that advances stepMs per batch.
func NewRecorder(sym Symbol, t0, stepMs int64) *Recorder {
	r := &Recorder{h: newHub(sym), now: t0, step: stepMs}
	r.h.nowMs = func() int64 { return r.now }
	// Every frame is kept, even one with no public events: the prev_seq chain
	// must stay unbroken or the client would (correctly) report a gap.
	r.h.capture = func(f Frame) { r.Frames = append(r.Frames, f) }
	return r
}

// OnEvents is an engine.Config.OnEvents callback.
func (r *Recorder) OnEvents(evs []book.Event) { r.now += r.step; r.h.apply(evs) }

// Reset forgets frames recorded so far (call after the warm-up phase).
func (r *Recorder) Reset() { r.Frames = nil }

// Snapshot returns the current depth view (depth levels per side).
func (r *Recorder) Snapshot(depth int) Snapshot { return r.h.Snapshot(depth) }

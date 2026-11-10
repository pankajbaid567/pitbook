package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func body(i int) []byte {
	return []byte(fmt.Sprintf("payload-%04d-%s", i, bytes.Repeat([]byte{'x'}, i%17)))
}

func fill(t *testing.T, dir string, n int, seg int64) {
	t.Helper()
	l, err := Open(Options{Dir: dir, SegmentBytes: seg})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if err := l.Append(uint64(i), 1, body(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func collect(t *testing.T, dir string, after uint64, tolerate bool) (seqs []uint64, err error) {
	t.Helper()
	_, err = Replay(dir, after, tolerate, func(seq uint64, typ byte, b []byte) error {
		if typ != 1 || !bytes.Equal(b, body(int(seq))) {
			t.Fatalf("record %d has wrong content", seq)
		}
		seqs = append(seqs, seq)
		return nil
	})
	return
}

func TestRoundTripAndReopen(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 100, 0)
	seqs, err := collect(t, dir, 0, false)
	if err != nil || len(seqs) != 100 || seqs[0] != 1 || seqs[99] != 100 {
		t.Fatalf("replay: n=%d err=%v", len(seqs), err)
	}
	l, err := Open(Options{Dir: dir})
	if err != nil || l.LastSeq() != 100 {
		t.Fatalf("reopen: last=%v err=%v", l, err)
	}
	if err := l.Append(100, 1, nil); err == nil {
		t.Fatal("duplicate seq must be refused")
	}
	if err := l.Append(101, 1, body(101)); err != nil {
		t.Fatal(err)
	}
	l.Close()
	seqs, _ = collect(t, dir, 95, false)
	if len(seqs) != 6 || seqs[0] != 96 {
		t.Fatalf("replay after 95: %v", seqs)
	}
}

func TestRotationAndTruncateBefore(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 300, 512) // many small segments
	segs, _ := listSegments(dir)
	if len(segs) < 5 {
		t.Fatalf("expected several segments, got %d", len(segs))
	}
	if seqs, err := collect(t, dir, 0, false); err != nil || len(seqs) != 300 {
		t.Fatalf("n=%d err=%v", len(seqs), err)
	}
	l, err := Open(Options{Dir: dir, SegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateBefore(200); err != nil {
		t.Fatal(err)
	}
	if l.FirstSeq() > 201 {
		t.Fatalf("truncated too much: first=%d", l.FirstSeq())
	}
	if l.FirstSeq() == 1 {
		t.Fatal("nothing was truncated")
	}
	l.Close()
	seqs, err := collect(t, dir, 200, false)
	if err != nil || len(seqs) != 100 || seqs[0] != 201 {
		t.Fatalf("after truncate: n=%d err=%v", len(seqs), err)
	}
}

// Cut the single segment at every byte offset: Open must always recover exactly
// the whole records before the cut, never more, never panic.
func TestTornTailAtEveryByteOffset(t *testing.T) {
	src := t.TempDir()
	fill(t, src, 25, 0)
	segs, _ := listSegments(src)
	whole, err := os.ReadFile(segs[0].path)
	if err != nil {
		t.Fatal(err)
	}
	// offsets where each record ends
	var ends []int
	off := 0
	for i := 1; i <= 25; i++ {
		off += frameHeader + 9 + len(body(i))
		ends = append(ends, off)
	}
	if off != len(whole) {
		t.Fatalf("size model wrong: %d vs %d", off, len(whole))
	}
	for cut := 0; cut <= len(whole); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(segs[0].path)), whole[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		want := 0
		for _, e := range ends {
			if e <= cut {
				want++
			}
		}
		l, err := Open(Options{Dir: dir})
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if int(l.LastSeq()) != want {
			t.Fatalf("cut %d: recovered %d records, want %d", cut, l.LastSeq(), want)
		}
		// The log must be appendable again right where it was cut.
		if err := l.Append(uint64(want+1), 1, body(want+1)); err != nil {
			t.Fatalf("cut %d: append after recovery: %v", cut, err)
		}
		l.Close()
		if seqs, err := collect(t, dir, 0, false); err != nil || len(seqs) != want+1 {
			t.Fatalf("cut %d: replay n=%d err=%v", cut, len(seqs), err)
		}
	}
}

func TestFlippedBitInLastSegmentIsTornTail(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 10, 0)
	segs, _ := listSegments(dir)
	b, _ := os.ReadFile(segs[0].path)
	b[len(b)-3] ^= 0x01 // corrupt the last record
	os.WriteFile(segs[0].path, b, 0o644)
	l, err := Open(Options{Dir: dir})
	if err != nil || l.LastSeq() != 9 {
		t.Fatalf("want recovery to 9, got last=%d err=%v", l.LastSeq(), err)
	}
	l.Close()
}

func TestCorruptEarlierSegmentIsRefused(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 200, 512)
	segs, _ := listSegments(dir)
	b, _ := os.ReadFile(segs[1].path)
	b[frameHeader+2] ^= 0xff
	os.WriteFile(segs[1].path, b, 0o644)
	if _, err := Open(Options{Dir: dir}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	if _, err := collect(t, dir, 0, true); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("replay must also refuse, got %v", err)
	}
}

func TestMissingMiddleSegmentIsRefused(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 200, 512)
	segs, _ := listSegments(dir)
	os.Remove(segs[1].path)
	if _, err := Open(Options{Dir: dir}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt for a gap, got %v", err)
	}
}

func TestReplayToleratesTornTailReadOnly(t *testing.T) {
	dir := t.TempDir()
	fill(t, dir, 10, 0)
	segs, _ := listSegments(dir)
	b, _ := os.ReadFile(segs[0].path)
	os.WriteFile(segs[0].path, b[:len(b)-5], 0o644)
	if seqs, err := collect(t, dir, 0, true); err != nil || len(seqs) != 9 {
		t.Fatalf("tolerant replay: n=%d err=%v", len(seqs), err)
	}
	if _, err := collect(t, dir, 0, false); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("strict replay must fail, got %v", err)
	}
	// Read-only replay must not have modified the file.
	b2, _ := os.ReadFile(segs[0].path)
	if len(b2) != len(b)-5 {
		t.Fatal("Replay modified the file")
	}
}

func TestRebaseEmptyLog(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(Options{Dir: dir})
	if err := l.Rebase(500); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(501, 1, body(501)); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(503, 1, nil); err == nil {
		t.Fatal("gap append must be refused")
	}
	l.Close()
	l, err := Open(Options{Dir: dir})
	if err != nil || l.LastSeq() != 501 {
		t.Fatalf("reopen: %v %v", l, err)
	}
	if err := l.Rebase(7); err == nil {
		t.Fatal("rebase of a non-empty log must fail")
	}
}

func FuzzOpenNeverPanics(f *testing.F) {
	dir := f.TempDir()
	l, _ := Open(Options{Dir: dir})
	for i := 1; i <= 5; i++ {
		l.Append(uint64(i), 1, body(i))
	}
	l.Close()
	segs, _ := listSegments(dir)
	seed, _ := os.ReadFile(segs[0].path)
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{9, 0, 0, 0, 1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := t.TempDir()
		os.WriteFile(filepath.Join(d, segName(1)), data, 0o644)
		l, err := Open(Options{Dir: d})
		if err != nil {
			return
		}
		// Whatever survived must be a consistent prefix that can be extended.
		if err := l.Append(l.LastSeq()+1, 1, []byte("z")); err != nil && l.LastSeq() != 0 {
			t.Fatalf("append after recovery: %v", err)
		}
		l.Close()
	})
}

package snapshot

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestWriteLatestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if _, _, _, err := Latest(dir); !errors.Is(err, ErrNone) {
		t.Fatalf("empty dir: %v", err)
	}
	if _, err := Write(dir, 10, []byte("ten")); err != nil {
		t.Fatal(err)
	}
	if _, err := Write(dir, 20, bytes.Repeat([]byte("z"), 5000)); err != nil {
		t.Fatal(err)
	}
	seq, p, skipped, err := Latest(dir)
	if err != nil || seq != 20 || len(p) != 5000 || len(skipped) != 0 {
		t.Fatalf("seq=%d len=%d skipped=%v err=%v", seq, len(p), skipped, err)
	}
}

func TestCorruptNewestFallsBackToOlder(t *testing.T) {
	dir := t.TempDir()
	Write(dir, 10, []byte("ten"))
	path, _ := Write(dir, 20, []byte("twenty"))
	b, _ := os.ReadFile(path)
	b[len(b)/2] ^= 0xff
	os.WriteFile(path, b, 0o644)
	seq, p, skipped, err := Latest(dir)
	if err != nil || seq != 10 || string(p) != "ten" || len(skipped) != 1 {
		t.Fatalf("seq=%d p=%q skipped=%v err=%v", seq, p, skipped, err)
	}
}

func TestEveryTruncationIsRejected(t *testing.T) {
	dir := t.TempDir()
	path, _ := Write(dir, 5, []byte("hello snapshot"))
	b, _ := os.ReadFile(path)
	for cut := 0; cut < len(b); cut++ {
		if _, _, err := decode(b[:cut]); err == nil {
			t.Fatalf("truncation at %d accepted", cut)
		}
	}
}

func TestNameSeqMismatchIsSkipped(t *testing.T) {
	dir := t.TempDir()
	path, _ := Write(dir, 5, []byte("x"))
	os.Rename(path, dir+"/"+name(9)) // content says 5, name says 9
	if _, _, skipped, err := Latest(dir); !errors.Is(err, ErrNone) || len(skipped) != 1 {
		t.Fatalf("err=%v skipped=%v", err, skipped)
	}
}

func TestPrune(t *testing.T) {
	dir := t.TempDir()
	for _, s := range []uint64{1, 2, 3, 4} {
		Write(dir, s, []byte("x"))
	}
	os.WriteFile(dir+"/snap-00000000000000000099.snap.tmp", []byte("junk"), 0o644)
	if err := Prune(dir, 2); err != nil {
		t.Fatal(err)
	}
	es, _ := list(dir)
	if len(es) != 2 || es[0].seq != 4 || es[1].seq != 3 {
		t.Fatalf("kept %v", es)
	}
	if m, _ := os.ReadDir(dir); len(m) != 2 {
		t.Fatalf("temp file not removed: %d files", len(m))
	}
}

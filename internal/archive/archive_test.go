package archive

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"pitbook/internal/book"
	"pitbook/internal/engine"
	"pitbook/internal/wal"
)

var ctx = context.Background()

func TestFileStoreContract(t *testing.T) {
	testStore(t, FileStore{Dir: t.TempDir()})
}

// fakeWorker implements the store Worker's HTTP contract in memory.
func fakeWorker(t *testing.T, token string) (*httptest.Server, *sync.Map) {
	var objs sync.Map
	h := http.NewServeMux()
	auth := func(w http.ResponseWriter, r *http.Request) bool {
		if !ConstantTimeEqual(r.Header.Get("Authorization"), "Bearer "+token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return false
		}
		return true
	}
	h.HandleFunc("/o/", func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/o/")
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			objs.Store(key, b)
		case http.MethodGet:
			v, ok := objs.Load(key)
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Write(v.([]byte))
		case http.MethodDelete:
			objs.Delete(key)
		}
	})
	h.HandleFunc("/l", func(w http.ResponseWriter, r *http.Request) {
		if !auth(w, r) {
			return
		}
		var keys []string
		objs.Range(func(k, _ any) bool {
			if strings.HasPrefix(k.(string), r.URL.Query().Get("prefix")) {
				keys = append(keys, k.(string))
			}
			return true
		})
		sort.Strings(keys)
		io.WriteString(w, strings.Join(keys, "\n"))
	})
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts, &objs
}

func TestHTTPStoreContractAndAuth(t *testing.T) {
	ts, _ := fakeWorker(t, "s3cret")
	testStore(t, NewHTTPStore(ts.URL, "s3cret"))
	bad := NewHTTPStore(ts.URL, "wrong")
	if err := bad.Put(ctx, "x/wal/a", []byte("1")); err == nil {
		t.Fatal("wrong token accepted")
	}
	if _, err := bad.List(ctx, ""); err == nil {
		t.Fatal("wrong token accepted for list")
	}
}

func testStore(t *testing.T, st Store) {
	t.Helper()
	if err := st.Put(ctx, "p/wal/wal-1.log", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	st.Put(ctx, "p/snap/snap-1.snap", []byte("snap"))
	st.Put(ctx, "q/wal/wal-9.log", []byte("other"))
	b, err := st.Get(ctx, "p/wal/wal-1.log")
	if err != nil || string(b) != "hello" {
		t.Fatalf("get: %q %v", b, err)
	}
	if _, err := st.Get(ctx, "p/nope"); err != ErrNotFound {
		t.Fatalf("missing key: %v", err)
	}
	keys, err := st.List(ctx, "p/")
	if err != nil || len(keys) != 2 {
		t.Fatalf("list: %v %v", keys, err)
	}
	if err := st.Delete(ctx, "p/wal/wal-1.log"); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, "p/wal/wal-1.log"); err != nil {
		t.Fatalf("deleting a missing key must not fail: %v", err)
	}
	if keys, _ := st.List(ctx, "p/"); len(keys) != 1 {
		t.Fatalf("after delete: %v", keys)
	}
}

func TestRestoreOnlyIntoEmptyDirAndOnlyKnownFiles(t *testing.T) {
	st := FileStore{Dir: t.TempDir()}
	st.Put(ctx, "PIT/wal/wal-00000000000000000001.log", []byte("w"))
	st.Put(ctx, "PIT/snap/snap-00000000000000000005.snap", []byte("s"))
	st.Put(ctx, "PIT/evil/../../x", []byte("no"))
	st.Put(ctx, "PIT/other/file.txt", []byte("ignored"))
	dir := t.TempDir()
	n, err := Restore(ctx, st, "PIT/", dir)
	if err != nil || n != 2 {
		t.Fatalf("restore n=%d err=%v", n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "wal", "wal-00000000000000000001.log")); string(b) != "w" {
		t.Fatal("wal not restored")
	}
	// Local state wins: a second restore must not touch anything.
	os.WriteFile(filepath.Join(dir, "wal", "wal-00000000000000000001.log"), []byte("local"), 0o644)
	n, err = Restore(ctx, st, "PIT/", dir)
	if err != nil || n != 0 {
		t.Fatalf("restore over existing state: n=%d err=%v", n, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "wal", "wal-00000000000000000001.log")); string(b) != "local" {
		t.Fatal("restore overwrote local state")
	}
}

func TestMirrorUploadsChangesAndDeletes(t *testing.T) {
	st := FileStore{Dir: t.TempDir()}
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "wal"), 0o755)
	os.MkdirAll(filepath.Join(dir, "snap"), 0o755)
	seg := filepath.Join(dir, "wal", "wal-00000000000000000001.log")
	os.WriteFile(seg, []byte("aaa"), 0o644)
	m := NewMirror(st, "PIT/", dir, time.Hour, nil)
	if err := m.SyncOnce(ctx); err != nil || m.Uploads != 1 {
		t.Fatalf("first sync: uploads=%d err=%v", m.Uploads, err)
	}
	if err := m.SyncOnce(ctx); err != nil || m.Uploads != 1 {
		t.Fatalf("unchanged file re-uploaded: %d", m.Uploads)
	}
	os.WriteFile(seg, []byte("aaaaaa"), 0o644) // active segment grew
	m.SyncOnce(ctx)
	if b, _ := st.Get(ctx, "PIT/wal/wal-00000000000000000001.log"); string(b) != "aaaaaa" || m.Uploads != 2 {
		t.Fatalf("grown file: %q uploads=%d", b, m.Uploads)
	}
	os.WriteFile(filepath.Join(dir, "wal", "ignored.tmp"), []byte("x"), 0o644)
	os.Remove(seg) // engine trimmed it
	os.WriteFile(filepath.Join(dir, "snap", "snap-00000000000000000009.snap"), []byte("s"), 0o644)
	m.SyncOnce(ctx)
	keys, _ := st.List(ctx, "PIT/")
	if len(keys) != 1 || keys[0] != "PIT/snap/snap-00000000000000000009.snap" || m.Deletes != 1 {
		t.Fatalf("after trim: %v deletes=%d", keys, m.Deletes)
	}
}

// The point of the package: a container that loses its disk comes back with the
// same state, byte for byte, by restoring from the bucket and replaying.
func TestWipedDiskRecoversFromBucket(t *testing.T) {
	ts, _ := fakeWorker(t, "tok")
	st := NewHTTPStore(ts.URL, "tok")
	dir1 := t.TempDir()
	cfg := engine.Config{Symbol: "PIT", Dir: dir1, WALMode: wal.ModeAsync, SnapshotEvery: 300, SegmentBytes: 8 << 10}
	e, _, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m := NewMirror(st, "PIT/", dir1, time.Hour, nil)
	for i := 1; i <= 2500; i++ {
		_, err := e.Exec(book.Command{Kind: book.CmdNew, Participant: uint32(i%7 + 1), ClientOID: uint64(i), Side: book.Side(i%2) + 1,
			Type: book.Limit, Price: book.Price(95 + i%11), Qty: book.Qty(i%5 + 1)}, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if i%400 == 0 {
			if err := m.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	var wantHash, wantSeq uint64
	e.Do(func(mc *engine.Machine) { wantHash, wantSeq = mc.Hash(), mc.LastSeq() })
	e.Close() // graceful: final snapshot
	if err := m.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}

	dir2 := t.TempDir() // a brand new container: empty disk
	n, err := Restore(ctx, st, "PIT/", dir2)
	if err != nil || n == 0 {
		t.Fatalf("restore: n=%d err=%v", n, err)
	}
	cfg.Dir = dir2
	e2, rec, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if rec.Hash != wantHash || rec.LastSeq != wantSeq {
		t.Fatalf("restored state differs: %+v want hash %x seq %d", rec, wantHash, wantSeq)
	}
	t.Logf("restored %d files, recovered seq %d (snapshot %d) identically", n, rec.LastSeq, rec.SnapshotSeq)
}

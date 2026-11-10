// Package archive mirrors a data directory (WAL segments and snapshots) to
// object storage and restores it on boot (ADR-009).
//
// Render's free tier has no persistent disk, so the hosted demo would restart
// empty. The engine still writes only to the local disk; this package copies
// those files to a bucket in the background and, on a fresh container, pulls
// them back before the engine opens. It is eventually durable (the active
// segment can be a few seconds behind), which is why durability claims are made
// only for the local-file WAL.
package archive

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Store is the minimal object-store surface the archiver needs.
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// ErrNotFound is returned by Get for a missing key.
var ErrNotFound = errors.New("archive: not found")

// ---- HTTPStore: talks to the Cloudflare Worker in deploy/worker-store ----

// HTTPStore is a Store backed by the store Worker (bearer-token protected).
type HTTPStore struct {
	Base   string // e.g. https://pitbook-store.example.workers.dev
	Token  string
	Client *http.Client
}

// NewHTTPStore returns a store with sane timeouts.
func NewHTTPStore(base, token string) *HTTPStore {
	return &HTTPStore{Base: strings.TrimRight(base, "/"), Token: token, Client: &http.Client{Timeout: 30 * time.Second}}
}

func (s *HTTPStore) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.Base+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	return s.Client.Do(req)
}

func drainClose(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
}

func (s *HTTPStore) Put(ctx context.Context, key string, data []byte) error {
	resp, err := s.do(ctx, http.MethodPut, "/o/"+key, data)
	if err != nil {
		return err
	}
	defer drainClose(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("archive: PUT %s: %s", key, resp.Status)
	}
	return nil
}

func (s *HTTPStore) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.do(ctx, http.MethodGet, "/o/"+key, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("archive: GET %s: %s", key, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 256<<20))
}

func (s *HTTPStore) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, "/o/"+key, nil)
	if err != nil {
		return err
	}
	defer drainClose(resp)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("archive: DELETE %s: %s", key, resp.Status)
	}
	return nil
}

func (s *HTTPStore) List(ctx context.Context, prefix string) ([]string, error) {
	resp, err := s.do(ctx, http.MethodGet, "/l?prefix="+url.QueryEscape(prefix), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("archive: LIST %s: %s", prefix, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if l != "" {
			keys = append(keys, l)
		}
	}
	return keys, nil
}

// ---- FileStore: a directory pretending to be a bucket (tests, local runs) ----

type FileStore struct{ Dir string }

func (f FileStore) path(key string) (string, error) {
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("archive: bad key %q", key)
	}
	return filepath.Join(f.Dir, filepath.FromSlash(key)), nil
}

func (f FileStore) Put(_ context.Context, key string, data []byte) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}

func (f FileStore) Get(_ context.Context, key string) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (f FileStore) Delete(_ context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (f FileStore) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	err := filepath.Walk(f.Dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(f.Dir, p)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefix) {
			out = append(out, rel)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// ---- restore ----

var subdirs = []string{"wal", "snap"}

func hasLocalState(dir string) bool {
	for _, sd := range subdirs {
		if es, err := os.ReadDir(filepath.Join(dir, sd)); err == nil && len(es) > 0 {
			return true
		}
	}
	return false
}

// Restore downloads every object under prefix into dir, but only if dir has no
// WAL or snapshot files yet (a fresh container). Local state always wins over
// the bucket. It returns the number of files written.
func Restore(ctx context.Context, st Store, prefix, dir string) (int, error) {
	if hasLocalState(dir) {
		return 0, nil
	}
	keys, err := st.List(ctx, prefix)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, k := range keys {
		rel := strings.TrimPrefix(k, prefix)
		if !validRel(rel) {
			continue
		}
		data, err := st.Get(ctx, k)
		if err != nil {
			return n, err
		}
		dst := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return n, err
		}
		// Written under a temp name and renamed, so a crash mid-restore never
		// leaves a half-written file that looks complete.
		tmp := dst + ".restore"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return n, err
		}
		if err := os.Rename(tmp, dst); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func validRel(rel string) bool {
	parts := strings.Split(rel, "/")
	if len(parts) != 2 || (parts[0] != "wal" && parts[0] != "snap") {
		return false
	}
	n := parts[1]
	return (strings.HasPrefix(n, "wal-") && strings.HasSuffix(n, ".log")) || (strings.HasPrefix(n, "snap-") && strings.HasSuffix(n, ".snap"))
}

// ---- mirror ----

// Mirror copies new and changed files to the store and removes remote files
// that no longer exist locally (the engine trims covered WAL segments and old
// snapshots). Create it only after Restore and after the engine has opened.
type Mirror struct {
	St       Store
	Prefix   string
	Dir      string
	Interval time.Duration
	Logf     func(string, ...any)

	mu       sync.Mutex
	uploaded map[string]int64 // rel -> size last uploaded
	lastSet  string           // names of local files at the last full pass; unchanged means no deletes needed
	Uploads  int
	Deletes  int
	LastErr  error
	LastOK   time.Time
}

// NewMirror returns a mirror with a 10 s default interval.
func NewMirror(st Store, prefix, dir string, interval time.Duration, logf func(string, ...any)) *Mirror {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Mirror{St: st, Prefix: prefix, Dir: dir, Interval: interval, Logf: logf, uploaded: map[string]int64{}}
}

// SyncOnce performs one pass.
func (m *Mirror) SyncOnce(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	local := map[string]bool{}
	for _, sd := range subdirs {
		es, err := os.ReadDir(filepath.Join(m.Dir, sd))
		if err != nil {
			continue
		}
		for _, e := range es {
			rel := sd + "/" + e.Name()
			if e.IsDir() || !validRel(rel) {
				continue
			}
			local[rel] = true
			info, err := e.Info()
			if err != nil {
				continue
			}
			if sz, ok := m.uploaded[rel]; ok && sz == info.Size() {
				continue
			}
			data, err := os.ReadFile(filepath.Join(m.Dir, filepath.FromSlash(rel)))
			if err != nil {
				continue // trimmed between listing and reading
			}
			if err := m.St.Put(ctx, m.Prefix+rel, data); err != nil {
				return err
			}
			m.uploaded[rel] = int64(len(data))
			m.Uploads++
		}
	}
	names := make([]string, 0, len(local))
	for k := range local {
		names = append(names, k)
	}
	sort.Strings(names)
	set := strings.Join(names, "\n")
	if set == m.lastSet {
		m.LastOK = time.Now()
		return nil // same files as last pass: nothing to delete remotely, skip the LIST request
	}
	remote, err := m.St.List(ctx, m.Prefix)
	if err != nil {
		return err
	}
	for _, k := range remote {
		rel := strings.TrimPrefix(k, m.Prefix)
		if validRel(rel) && !local[rel] {
			if err := m.St.Delete(ctx, k); err != nil {
				return err
			}
			delete(m.uploaded, rel)
			m.Deletes++
		}
	}
	m.lastSet = set
	m.LastOK = time.Now()
	return nil
}

// Run syncs every Interval until ctx is done, then once more.
func (m *Mirror) Run(ctx context.Context) {
	t := time.NewTicker(m.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := m.SyncOnce(fctx)
			cancel()
			if err != nil {
				m.Logf("archive: final sync failed: %v", err)
			}
			return
		case <-t.C:
			if err := m.SyncOnce(ctx); err != nil && ctx.Err() == nil {
				m.mu.Lock()
				m.LastErr = err
				m.mu.Unlock()
				m.Logf("archive: sync failed: %v", err)
			}
		}
	}
}

// Status returns counters for /healthz.
func (m *Mirror) Status() (uploads, deletes int, lastOK time.Time, lastErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Uploads, m.Deletes, m.LastOK, m.LastErr
}

// ConstantTimeEqual reports whether a and b are equal without leaking length
// or content through timing (used by tests of the Worker contract).
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

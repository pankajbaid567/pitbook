// Package wal is an append-only, CRC-framed, segmented write-ahead log
// (ADR-007).
//
// Record layout, little endian:
//
//	len   u32   length of the payload that follows
//	crc   u32   CRC-32C (Castagnoli) of the payload
//	payload:    seq u64 | type u8 | body
//
// Segments are files named wal-<first seq, 20 digits>.log. Sequence numbers are
// strictly consecutive across the whole log. A bad or short record at the end
// of the last segment is a torn tail (a crash mid-write) and is truncated on
// Open; the same damage in an earlier segment is corruption and Open fails.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Mode controls durability.
type Mode int

const (
	// ModeSync: Sync flushes and fsyncs. Acknowledge only after Sync returns.
	ModeSync Mode = iota
	// ModeAsync: Sync flushes to the OS but does not fsync. Never use it for a
	// durability claim; it exists for benchmarks.
	ModeAsync
)

// Options configure Open.
type Options struct {
	Dir          string
	SegmentBytes int64 // rotate when the active segment reaches this size; default 64 MiB
	Mode         Mode
}

// ErrCorrupt reports damage that is not a torn tail.
var ErrCorrupt = errors.New("wal: corrupt")

const (
	frameHeader = 8
	maxPayload  = 1 << 20
	minPayload  = 9 // seq + type
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type segment struct {
	first uint64
	path  string
}

// Log is a write-ahead log. It is not safe for concurrent use: the engine
// goroutine owns it.
type Log struct {
	opts    Options
	segs    []segment // sorted by first
	f       *os.File  // active (last) segment, nil until first Append
	w       *bufio.Writer
	segSize int64
	lastSeq uint64
	scratch []byte
}

func segName(first uint64) string { return fmt.Sprintf("wal-%020d.log", first) }

func listSegments(dir string) ([]segment, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []segment
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, "wal-") || !strings.HasSuffix(n, ".log") {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, "wal-"), ".log"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: bad segment name %q", ErrCorrupt, n)
		}
		segs = append(segs, segment{first: v, path: filepath.Join(dir, n)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].first < segs[j].first })
	return segs, nil
}

// scanResult describes one segment file.
type scanResult struct {
	count     int
	first     uint64
	last      uint64
	validEnd  int64 // offset after the last good record
	fileSize  int64
	damaged   bool // bytes exist after validEnd
	damageWhy string
}

// scanSegment reads path, calls fn for each good record and stops at the first
// bad one. fn may be nil. An error from fn aborts the scan.
func scanSegment(path string, fn func(seq uint64, typ byte, body []byte) error) (scanResult, error) {
	var res scanResult
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return res, err
	}
	res.fileSize = st.Size()
	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [frameHeader]byte
	buf := make([]byte, 0, 4096)
	var off int64
	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			res.damaged, res.damageWhy = true, "short header"
			break
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		want := binary.LittleEndian.Uint32(hdr[4:])
		if n < minPayload || n > maxPayload {
			res.damaged, res.damageWhy = true, fmt.Sprintf("bad length %d", n)
			break
		}
		if cap(buf) < int(n) {
			buf = make([]byte, n)
		}
		buf = buf[:n]
		if _, err := io.ReadFull(r, buf); err != nil {
			res.damaged, res.damageWhy = true, "short payload"
			break
		}
		if crc32.Checksum(buf, castagnoli) != want {
			res.damaged, res.damageWhy = true, "crc mismatch"
			break
		}
		seq := binary.LittleEndian.Uint64(buf)
		if res.count > 0 && seq != res.last+1 {
			res.damaged, res.damageWhy = true, fmt.Sprintf("sequence jump %d -> %d", res.last, seq)
			break
		}
		if res.count == 0 {
			res.first = seq
		}
		if fn != nil {
			if err := fn(seq, buf[8], buf[9:]); err != nil {
				return res, err
			}
		}
		res.count++
		res.last = seq
		off += frameHeader + int64(n)
		res.validEnd = off
	}
	return res, nil
}

// Open scans the log, truncates a torn tail on the last segment, and returns a
// Log ready to append after LastSeq.
func Open(opts Options) (*Log, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}
	l := &Log{opts: opts, segs: segs}
	var prevLast uint64
	for i, s := range segs {
		res, err := scanSegment(s.path, nil)
		if err != nil {
			return nil, err
		}
		isLast := i == len(segs)-1
		if res.damaged && !isLast {
			return nil, fmt.Errorf("%w: %s: %s at offset %d (not the last segment)", ErrCorrupt, filepath.Base(s.path), res.damageWhy, res.validEnd)
		}
		if res.count > 0 && res.first != s.first {
			return nil, fmt.Errorf("%w: %s: first record is %d", ErrCorrupt, filepath.Base(s.path), res.first)
		}
		if i > 0 && s.first != prevLast+1 {
			return nil, fmt.Errorf("%w: gap before %s (previous segment ended at %d)", ErrCorrupt, filepath.Base(s.path), prevLast)
		}
		if res.count == 0 && !isLast {
			return nil, fmt.Errorf("%w: empty non-last segment %s", ErrCorrupt, filepath.Base(s.path))
		}
		if res.count > 0 {
			prevLast = res.last
		} else {
			prevLast = s.first - 1
		}
		if isLast {
			f, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
			if err != nil {
				return nil, err
			}
			if res.damaged || res.validEnd != res.fileSize {
				if err := f.Truncate(res.validEnd); err != nil {
					f.Close()
					return nil, err
				}
				if err := f.Sync(); err != nil {
					f.Close()
					return nil, err
				}
			}
			if _, err := f.Seek(res.validEnd, io.SeekStart); err != nil {
				f.Close()
				return nil, err
			}
			l.f, l.w, l.segSize = f, bufio.NewWriterSize(f, 256<<10), res.validEnd
		}
	}
	l.lastSeq = prevLast
	if len(segs) == 0 {
		l.lastSeq = 0
	}
	return l, nil
}

// LastSeq returns the sequence number of the last record (0 if empty).
func (l *Log) LastSeq() uint64 { return l.lastSeq }

// FirstSeq returns the first sequence number still on disk (0 if empty).
func (l *Log) FirstSeq() uint64 {
	if len(l.segs) == 0 {
		return 0
	}
	return l.segs[0].first
}

// Rebase tells an empty log that the state it protects already covers every
// sequence up to seq (it was restored from a snapshot), so the next Append
// must be seq+1.
func (l *Log) Rebase(seq uint64) error {
	if len(l.segs) != 0 && l.lastSeq != seq {
		return fmt.Errorf("wal: cannot rebase a non-empty log (last %d) to %d", l.lastSeq, seq)
	}
	l.lastSeq = seq
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (l *Log) rotate(first uint64) error {
	if l.f != nil {
		if err := l.w.Flush(); err != nil {
			return err
		}
		if l.opts.Mode == ModeSync {
			if err := l.f.Sync(); err != nil {
				return err
			}
		}
		if err := l.f.Close(); err != nil {
			return err
		}
		l.f, l.w = nil, nil
	}
	path := filepath.Join(l.opts.Dir, segName(first))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if l.opts.Mode == ModeSync {
		if err := syncDir(l.opts.Dir); err != nil {
			f.Close()
			return err
		}
	}
	l.f, l.w, l.segSize = f, bufio.NewWriterSize(f, 256<<10), 0
	l.segs = append(l.segs, segment{first: first, path: path})
	return nil
}

// Append buffers one record. It is durable only after Sync (ModeSync).
func (l *Log) Append(seq uint64, typ byte, body []byte) error {
	if l.lastSeq != 0 && seq != l.lastSeq+1 {
		return fmt.Errorf("wal: append seq %d, want %d", seq, l.lastSeq+1)
	}
	n := 8 + 1 + len(body)
	if n > maxPayload {
		return fmt.Errorf("wal: record too large (%d bytes)", n)
	}
	if l.f == nil || l.segSize >= l.opts.SegmentBytes {
		if err := l.rotate(seq); err != nil {
			return err
		}
	}
	need := frameHeader + n
	if cap(l.scratch) < need {
		l.scratch = make([]byte, need)
	}
	b := l.scratch[:need]
	binary.LittleEndian.PutUint32(b[0:], uint32(n))
	binary.LittleEndian.PutUint64(b[8:], seq)
	b[16] = typ
	copy(b[17:], body)
	binary.LittleEndian.PutUint32(b[4:], crc32.Checksum(b[8:], castagnoli))
	if _, err := l.w.Write(b); err != nil {
		return err
	}
	l.segSize += int64(need)
	l.lastSeq = seq
	return nil
}

// Sync pushes buffered records to the OS and, in ModeSync, to stable storage.
func (l *Log) Sync() error {
	if l.f == nil {
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if l.opts.Mode == ModeSync {
		return l.f.Sync()
	}
	return nil
}

// TruncateBefore deletes whole segments whose records are all <= seq. The
// active segment is never deleted.
func (l *Log) TruncateBefore(seq uint64) error {
	removed := false
	for len(l.segs) > 1 && l.segs[1].first <= seq+1 {
		if err := os.Remove(l.segs[0].path); err != nil {
			return err
		}
		l.segs = l.segs[1:]
		removed = true
	}
	if removed {
		return syncDir(l.opts.Dir)
	}
	return nil
}

// Close flushes and closes the active segment.
func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	err := l.Sync()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f, l.w = nil, nil
	return err
}

// Replay streams every record with seq > after, in order, from the segments in
// dir. With tolerateTornTail a damaged end of the last segment is ignored
// (read-only inspection of a log a crashed process left behind); otherwise any
// damage is ErrCorrupt. It returns the last sequence delivered (or after if
// none).
func Replay(dir string, after uint64, tolerateTornTail bool, fn func(seq uint64, typ byte, body []byte) error) (uint64, error) {
	segs, err := listSegments(dir)
	if err != nil {
		return after, err
	}
	last := after
	for i, s := range segs {
		if i+1 < len(segs) && segs[i+1].first <= after+1 {
			continue // everything in this segment is <= after
		}
		res, err := scanSegment(s.path, func(seq uint64, typ byte, body []byte) error {
			if seq <= after {
				return nil
			}
			if seq != last+1 && (last != after || seq != after+1) {
				return fmt.Errorf("%w: sequence jump %d -> %d", ErrCorrupt, last, seq)
			}
			last = seq
			return fn(seq, typ, body)
		})
		if err != nil {
			return last, err
		}
		if res.damaged && !(tolerateTornTail && i == len(segs)-1) {
			return last, fmt.Errorf("%w: %s: %s at offset %d", ErrCorrupt, filepath.Base(s.path), res.damageWhy, res.validEnd)
		}
	}
	return last, nil
}

// CloseNoFlush closes the active segment without flushing buffered records. It
// exists to simulate a crash in tests and must not be used otherwise.
func (l *Log) CloseNoFlush() {
	if l.f != nil {
		l.f.Close()
		l.f, l.w = nil, nil
	}
}

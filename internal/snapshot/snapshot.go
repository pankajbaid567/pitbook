// Package snapshot writes and reads atomic, checksummed state snapshots
// (ADR-008).
//
// File snap-<seq 20 digits>.snap:
//
//	magic "PBSN1\n" | seq u64 | len u64 | payload | crc32c u32 (over everything before it)
//
// A snapshot appears under its final name only after its bytes and its
// directory entry are on stable storage (write temp, fsync, rename, fsync dir).
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	magic      = []byte("PBSN1\n")
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	// ErrNone means no valid snapshot exists.
	ErrNone = errors.New("snapshot: none")
)

func name(seq uint64) string { return fmt.Sprintf("snap-%020d.snap", seq) }

func encode(seq uint64, payload []byte) []byte {
	b := make([]byte, 0, len(magic)+16+len(payload)+4)
	b = append(b, magic...)
	b = binary.LittleEndian.AppendUint64(b, seq)
	b = binary.LittleEndian.AppendUint64(b, uint64(len(payload)))
	b = append(b, payload...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func decode(b []byte) (seq uint64, payload []byte, err error) {
	if len(b) < len(magic)+16+4 || string(b[:len(magic)]) != string(magic) {
		return 0, nil, errors.New("bad header")
	}
	if crc32.Checksum(b[:len(b)-4], castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return 0, nil, errors.New("crc mismatch")
	}
	seq = binary.LittleEndian.Uint64(b[len(magic):])
	n := binary.LittleEndian.Uint64(b[len(magic)+8:])
	if n != uint64(len(b)-len(magic)-16-4) {
		return 0, nil, errors.New("length mismatch")
	}
	return seq, b[len(magic)+16 : len(b)-4], nil
}

// Write stores a snapshot atomically and returns its path.
func Write(dir string, seq uint64, payload []byte) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	final := filepath.Join(dir, name(seq))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(encode(seq, payload)); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", err
	}
	d, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	defer d.Close()
	return final, d.Sync()
}

type entry struct {
	seq  uint64
	path string
}

func list(dir string) ([]entry, error) {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []entry
	for _, e := range ents {
		n := e.Name()
		if !strings.HasPrefix(n, "snap-") || !strings.HasSuffix(n, ".snap") {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(n, "snap-"), ".snap"), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, entry{v, filepath.Join(dir, n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq > out[j].seq }) // newest first
	return out, nil
}

// Latest returns the newest snapshot that passes its checksum. Damaged newer
// snapshots are skipped (their paths are returned in skipped) and the next
// older one is tried. ErrNone if there is no usable snapshot.
func Latest(dir string) (seq uint64, payload []byte, skipped []string, err error) {
	es, err := list(dir)
	if err != nil {
		return 0, nil, nil, err
	}
	for _, e := range es {
		b, rerr := os.ReadFile(e.path)
		if rerr == nil {
			s, p, derr := decode(b)
			if derr == nil && s == e.seq {
				return s, p, skipped, nil
			}
		}
		skipped = append(skipped, e.path)
	}
	return 0, nil, skipped, ErrNone
}

// Prune keeps the newest keep snapshots and removes the rest (and stale temp
// files).
func Prune(dir string, keep int) error {
	es, err := list(dir)
	if err != nil {
		return err
	}
	for i, e := range es {
		if i >= keep {
			if err := os.Remove(e.path); err != nil {
				return err
			}
		}
	}
	tmps, _ := filepath.Glob(filepath.Join(dir, "snap-*.tmp"))
	for _, t := range tmps {
		os.Remove(t)
	}
	return nil
}

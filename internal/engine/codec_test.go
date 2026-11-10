package engine

import (
	"bytes"
	"testing"

	"pitbook/internal/book"
)

func TestCodecRoundTrip(t *testing.T) {
	c := book.Command{Kind: book.CmdReplace, Side: book.Sell, Type: book.Limit, Participant: 0xdeadbeef,
		ClientOID: 1<<63 + 5, Price: 12345, Qty: -7, Target: 99}
	b := EncodeCommand(nil, c)
	if len(b) != CommandSize {
		t.Fatalf("size %d", len(b))
	}
	got, err := DecodeCommand(b)
	if err != nil || got != c {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	good := EncodeCommand(nil, book.Command{Kind: book.CmdNew, Side: book.Buy, Type: book.Limit, Price: 1, Qty: 1})
	for name, b := range map[string][]byte{
		"short": good[:39], "long": append(append([]byte{}, good...), 0), "empty": nil,
	} {
		if _, err := DecodeCommand(b); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	for _, k := range []byte{0, 4, 255} {
		b := append([]byte{}, good...)
		b[0] = k
		if _, err := DecodeCommand(b); err == nil {
			t.Fatalf("kind %d accepted", k)
		}
	}
	b := append([]byte{}, good...)
	b[3] = 1
	if _, err := DecodeCommand(b); err == nil {
		t.Fatal("non-zero reserved byte accepted")
	}
}

// FuzzDecodeCommand: the decoder never panics, and anything it accepts
// re-encodes to exactly the same bytes (canonical form).
func FuzzDecodeCommand(f *testing.F) {
	f.Add(EncodeCommand(nil, book.Command{Kind: book.CmdNew, Side: book.Buy, Type: book.Limit, Participant: 1, ClientOID: 2, Price: 3, Qty: 4}))
	f.Add(make([]byte, 40))
	f.Add([]byte("garbage"))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := DecodeCommand(b)
		if err != nil {
			return
		}
		if re := EncodeCommand(nil, c); !bytes.Equal(re, b) {
			t.Fatalf("not canonical: %x -> %+v -> %x", b, c, re)
		}
	})
}

package engine

import (
	"encoding/binary"
	"errors"

	"pitbook/internal/book"
)

// CommandSize is the fixed size of an encoded command.
const CommandSize = 40

// RecCommand is the WAL record type of a command.
const RecCommand byte = 1

// ErrBadCommand is returned for malformed encoded commands.
var ErrBadCommand = errors.New("engine: bad command encoding")

// EncodeCommand appends the canonical 40-byte little-endian encoding of c:
//
//	0 kind | 1 side | 2 type | 3 reserved(0) | 4 participant u32 |
//	8 client oid u64 | 16 price i64 | 24 qty i64 | 32 target u64
func EncodeCommand(dst []byte, c book.Command) []byte {
	dst = append(dst, byte(c.Kind), byte(c.Side), byte(c.Type), 0)
	dst = binary.LittleEndian.AppendUint32(dst, c.Participant)
	dst = binary.LittleEndian.AppendUint64(dst, c.ClientOID)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(c.Price))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(c.Qty))
	return binary.LittleEndian.AppendUint64(dst, uint64(c.Target))
}

// DecodeCommand parses EncodeCommand output. It is strict: wrong length, an
// unknown kind or a non-zero reserved byte are errors, so that every accepted
// input re-encodes to identical bytes. Invalid field values (for example a
// zero quantity) are accepted here; the book rejects them deterministically.
func DecodeCommand(b []byte) (book.Command, error) {
	if len(b) != CommandSize || b[3] != 0 || b[0] < byte(book.CmdNew) || b[0] > byte(book.CmdReplace) {
		return book.Command{}, ErrBadCommand
	}
	return book.Command{
		Kind:        book.CmdKind(b[0]),
		Side:        book.Side(b[1]),
		Type:        book.OrderType(b[2]),
		Participant: binary.LittleEndian.Uint32(b[4:]),
		ClientOID:   binary.LittleEndian.Uint64(b[8:]),
		Price:       book.Price(binary.LittleEndian.Uint64(b[16:])),
		Qty:         book.Qty(binary.LittleEndian.Uint64(b[24:])),
		Target:      book.OrderID(binary.LittleEndian.Uint64(b[32:])),
	}, nil
}

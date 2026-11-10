// Package book is the deterministic matching core.
//
// Rules (see docs/DESIGN.md ADR-002, ADR-003):
//   - no wall-clock time, no randomness, no I/O, no goroutines;
//   - timestamps and trade IDs come from the sequencer, never from the host;
//   - prices are integer ticks, quantities are integer lots, never floats;
//   - the same command sequence must always produce the same events.
package book

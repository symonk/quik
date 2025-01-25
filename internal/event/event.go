package event

import "io"

type EventType int

const (
	WorkerStarted EventType = iota
	WorkerStopped
)

// Event encapsulates a pool emitted event.
// for now it writes events to some arbitrary
// sync
//
// This is useful for debugging at scale just for now.
type Event struct {
	t EventType
	w io.Writer
}

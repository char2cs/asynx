package asynx

import (
	"context"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
)

// EventListener consumes events without registering a handler function:
// through a channel, by blocking until one arrives, or by replaying the
// history of one aggregate. Unlike Subscribe, whose pattern is an exact event
// name or a raw regular expression, Listen and SubscribeWait take the readable
// pattern format of Topic.
//
// Listen and SubscribeWait see only events published after they were called;
// they never replay. Replay is the way to read the past.
type EventListener[T any] interface {
	// Listen opens a channel-based subscription for events matching the given pattern.
	// The pattern is converted via Topic() internally.
	//
	// count > 0: the channel has capacity equal to count, auto-closes and
	//            auto-unsubscribes after count events are received.
	// count == 0: unbounded — the channel has a default capacity of 16; it never
	//             auto-closes. The caller must call the returned unsubscribe func
	//             to clean up. Note: unsub() does NOT close the channel in
	//             unbounded mode; do not range over the channel after calling unsub.
	// count < 0: treated the same as count == 0 (unbounded, capacity 16).
	//
	// The returned unsubscribe func is idempotent.
	// Returns ErrEmptyPattern if pattern is empty.
	Listen(
		pattern string,
		count int,
	) (<-chan event.Event[T], func(), error)

	// SubscribeWait blocks until the first event matching pattern arrives or ctx
	// is cancelled. The pattern is converted via Topic() internally (through Listen).
	//
	// Returns the matching event, or ctx.Err() if the context is cancelled or its
	// deadline is exceeded. Auto-unsubscribes in all cases.
	//
	// To bound the wait time, pass a context with a deadline (e.g.,
	// context.WithTimeout(parent, 5*time.Second)); pass context.Background() to
	// wait indefinitely.
	//
	// For subscribe-before-act ordering (subscribe → check state → send command →
	// then read), use Listen directly.
	SubscribeWait(
		ctx context.Context,
		pattern string,
	) (event.Event[T], error)

	// Replay feeds the events of the aggregate to fn, in version order, each
	// upcast to the current schema version and carrying both the new and the
	// previous state. It reads the store directly, so it sees committed events
	// whether or not a subscriber got them, and it is read-only: it writes no
	// snapshot and delivers nothing to subscribers.
	//
	// fromVersion is the first version passed to fn (1 starts at the
	// beginning) and toVersion the last one, with 0 meaning to the end of the
	// stream. Earlier events are still read so that the first event's previous
	// state is correct, but fn is not called for them. It returns the error of
	// the store, of an upcaster, or ctx's error.
	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn handler.Projection[T],
	) error
}

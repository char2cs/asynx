package asynx

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// EventListener consumes events through channels, blocking waits and replays.
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
	) (<-chan models.Event[T], func(), error)

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
	) (models.Event[T], error)

	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn models.ProjectionHandler[T],
	) error
}

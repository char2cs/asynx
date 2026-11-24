package asynx

import (
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"
)

// SubscriptionManager registers and removes the live handlers of an instance:
// the projection handlers that Subscribe adds for event names and the forget
// handlers that OnForget adds for deleted aggregates.
//
// Live handlers only see events published while the instance is running and
// cannot report failure. A subscriber that must see every committed event, with
// retries, belongs to Builder.WithSubscriber instead. Both kinds of
// registration return an ID that Unsubscribe removes.
type SubscriptionManager[T any] interface {
	// Subscribe registers handler for every event whose name matches pattern
	// and returns the ID to pass to Unsubscribe. A pattern without regular
	// expression metacharacters matches an event name exactly; any other
	// pattern is a regular expression matched anywhere in the name, so anchor
	// it to match the whole name (Topic builds an anchored one from a readable
	// pattern). opts may set a per-handler timeout and a fallback handler.
	//
	// Handlers run asynchronously after the event is committed, so when Send
	// returns they may not have run; SendWait waits for them. A handler that
	// panics is reported to the panic handler (see Builder.WithPanicHandler).
	//
	// It returns failure.ErrEmptyPattern for an empty pattern,
	// failure.ErrNilHandler for a nil handler and failure.ErrBusClosed once the
	// instance is shut down.
	Subscribe(
		pattern string,
		handler handler.Projection[T],
		opts ...subscription.Opt[T],
	) (string, error)

	// Unsubscribe removes the subscription with the given ID, as returned by
	// Subscribe, OnForget or Listen. Handlers already running are not
	// interrupted. An unknown ID is not an error with the default bus; a custom
	// bus (Builder.WithBus) decides what it returns.
	Unsubscribe(
		id string,
	) error

	// OnForget registers a handler invoked when any aggregate is forgotten (see
	// Forget). The handler receives the tombstone event, whose Aggregate holds
	// the last known state, so it can erase what it derived from the
	// aggregate. Returns an ID that can be passed to
	// Unsubscribe, or the error Subscribe would return. Handlers registered
	// before Build with Builder.WithForgetHandler are equivalent.
	OnForget(
		fn handler.Forget[T],
	) (string, error)
}

package models

// BusSubscriber registers and removes the handlers a Bus delivers to.
type BusSubscriber[T any] interface {
	// Subscribe registers handler for every event whose name matches pattern
	// and returns the id to pass to Unsubscribe. A pattern without regular
	// expression metacharacters matches the event name exactly; any other
	// pattern is a regular expression matched anywhere in the name, so anchor
	// it to match the whole name. opts set per-subscription options such as a
	// timeout (WithHandlerTimeout) and a fallback handler (WithFallback).
	//
	// It returns ErrEmptyPattern for an empty pattern, ErrNilHandler for a nil
	// handler and ErrBusClosed once the bus is closed. A handler added while
	// an event is being published sees that event only if the publish had not
	// yet taken its snapshot of the subscriptions.
	Subscribe(
		pattern string,
		handler ProjectionHandler[T],
		opts ...SubscriptionOpt[T],
	) (string, error)

	// Unsubscribe removes the subscription with the given id, so events
	// published afterwards no longer reach its handler. Handlers already
	// started for earlier events run to completion. An unknown or already
	// removed id is not an error, which makes the call idempotent.
	Unsubscribe(
		id string,
	) error
}

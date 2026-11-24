package models

import (
	"context"
)

// BusPublisher hands events to the handlers whose subscription matches the
// event's EventName.
//
// Neither method is a persistence step: both are called only after the event
// is durably written to the store, so a failure to publish never means the
// event was lost.
type BusPublisher[T any] interface {
	// Publish starts every matching handler, each on its own goroutine, and
	// returns without waiting for them. The event is dispatched to the
	// handlers as given, and ctx reaches each handler unchanged: cancelling it
	// is the handler's signal to stop, it does not abort handlers already
	// started.
	//
	// A non-nil error means dispatch failed before anything was started: ctx
	// was already done (its error is returned) or the bus is closed
	// (ErrBusClosed). Publishing an event nothing subscribes to succeeds.
	// Handlers of one event run concurrently and in no defined order.
	Publish(
		ctx context.Context,
		event Event[T],
	) error

	// PublishSync starts the matching handlers like Publish and blocks until
	// every handler goroutine triggered by this specific event has completed,
	// including one that exceeded its timeout: the timeout is reported, but
	// the call keeps waiting. It does not wait for handlers of other,
	// concurrent events. The dispatcher relies on this so that the next event
	// of an aggregate never starts while the previous one is still handled.
	//
	// Its errors are those of Publish and are returned before anything is
	// started; handler panics and timeouts do not affect them. Waiting for a
	// handler that is itself waiting on the caller deadlocks.
	PublishSync(
		ctx context.Context,
		event Event[T],
	) error
}

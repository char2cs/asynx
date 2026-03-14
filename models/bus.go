package models

import "context"

type Bus[T any] interface {
	// Publish is called only after the event is durably written to the store.
	// A non-nil error means dispatch failed, not that the event was lost.
	Publish(
		ctx context.Context,
		event Event[T],
	) error

	Subscribe(
		pattern string,
		handler ProjectionHandler[T],
		opts ...SubscriptionOpt[T],
	) (string, error)

	Unsubscribe(
		id string,
	) error

	Close(
		ctx context.Context,
	) error

	// WaitForHandlers blocks until all in-flight handler executions complete.
	// Only for use in tests; do not call in production code.
	WaitForHandlers()
}

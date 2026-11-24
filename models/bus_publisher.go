package models

import (
	"context"
)

// BusPublisher publishes events to the matching handlers.
type BusPublisher[T any] interface {
	// Publish is called only after the event is durably written to the store.
	// A non-nil error means dispatch failed, not that the event was lost.
	Publish(
		ctx context.Context,
		event Event[T],
	) error

	// PublishSync fires all matching handlers synchronously — it blocks until every
	// handler goroutine triggered by this specific event has completed. It does NOT
	// wait for handlers from other concurrent events.
	PublishSync(
		ctx context.Context,
		event Event[T],
	) error
}

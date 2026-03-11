package asynx

import (
	"context"
	"time"
)

type Bus[T any] interface {
	// Publish is called only after the event is durably written to the store.
	// A non-nil error means dispatch failed, not that the event was lost.
	Publish(
		ctx context.Context,
		event Event[T],
	) error

	Subscribe(
		pattern string,
		handler func(Event[T]),
		opts ...SubscriptionOpt[T],
	) (string, error)

	Unsubscribe(
		id string,
	) error

	Close(
		ctx context.Context,
	) error
}

type Store interface {
	// Append enforces (aggregateID, version) uniqueness — the sole coordination
	// mechanism for correctness across concurrent writers.
	Append(
		ctx context.Context,
		aggregateID string,
		version int64,
		data []byte,
	) error

	ReadFrom(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
	) ([][]byte, error)
	ReadRange(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)
}

type SubscriptionOpt[T any] func(*SubscriptionConfig[T])

type SubscriptionConfig[T any] struct {
	Fallback func(Event[T])
	Timeout  time.Duration
}

// WithFallback registers a secondary handler invoked when the primary panics.
func WithFallback[T any](
	handler func(Event[T]),
) SubscriptionOpt[T] {
	return func(cfg *SubscriptionConfig[T]) {
		cfg.Fallback = handler
	}
}

// WithHandlerTimeout sets the maximum duration a subscription handler may run.
func WithHandlerTimeout[T any](
	d time.Duration,
) SubscriptionOpt[T] {
	return func(cfg *SubscriptionConfig[T]) {
		cfg.Timeout = d
	}
}

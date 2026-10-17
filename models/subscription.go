package models

import "time"

type SubscriptionOpt[T any] func(*SubscriptionConfig[T])

type SubscriptionConfig[T any] struct {
	Fallback ProjectionHandler[T]
	Timeout  time.Duration
}

// WithFallback registers a secondary handler invoked when the primary panics.
func WithFallback[T any](
	handler ProjectionHandler[T],
) SubscriptionOpt[T] {
	return func(cfg *SubscriptionConfig[T]) {
		cfg.Fallback = handler
	}
}

// WithHandlerTimeout sets the duration after which a subscription handler is
// reported as timed out. On the dispatcher path (every event committed through
// asynx) the handler is still waited for, so the aggregate's next event never
// overlaps it; the timeout is an alarm, not a cancellation.
func WithHandlerTimeout[T any](
	d time.Duration,
) SubscriptionOpt[T] {
	return func(cfg *SubscriptionConfig[T]) {
		cfg.Timeout = d
	}
}

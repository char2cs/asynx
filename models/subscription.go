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

// WithHandlerTimeout sets the maximum duration a subscription handler may run.
func WithHandlerTimeout[T any](
	d time.Duration,
) SubscriptionOpt[T] {
	return func(cfg *SubscriptionConfig[T]) {
		cfg.Timeout = d
	}
}

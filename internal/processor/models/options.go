package models

import (
	"context"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// WithDurableSubscribers registers the durable subscribers the dispatcher runs,
// with retry, for durable deliveries.
func WithDurableSubscribers[T any](
	subs []asynxmd.DurableSub[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.DurableSubs = subs
	}
}

// WithDeliveryErrorHandler observes failed durable delivery attempts.
func WithDeliveryErrorHandler[T any](
	fn asynxmd.DeliveryErrorHandler[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.OnDeliveryError = fn
	}
}

// WithDeliveryPanicHandler is called when a durable handler panics.
func WithDeliveryPanicHandler[T any](
	fn asynxmd.PanicHandler[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.OnPanic = fn
	}
}

// WithDeliveryMaxAttempts caps the attempts of a failing durable delivery. When
// the cap is reached the delivery goes to the dead-letter handler if one is set
// and accepts it; otherwise the aggregate is parked. Non-positive values are
// ignored, leaving the default of 20.
func WithDeliveryMaxAttempts[T any](
	n int,
) Opt[T] {
	if n <= 0 {
		return ignored[T]
	}

	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.MaxAttempts = n
	}
}

// WithDeadLetterHandler sets the handler for deliveries that ran out of attempts.
func WithDeadLetterHandler[T any](
	fn asynxmd.DeadLetterHandler[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.DeadLetter = fn
	}
}

// WithDeliveryRetry sets the delay before a failed durable delivery is retried:
// it starts at initial and doubles up to max. Non-positive values are ignored,
// each leaving its default (100ms and 30s).
func WithDeliveryRetry[T any](
	initial time.Duration,
	max time.Duration,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.RetryInitial = positiveOr(
			initial,
			cfg.Dispatcher.RetryInitial,
		)
		cfg.Dispatcher.RetryMax = positiveOr(
			max,
			cfg.Dispatcher.RetryMax,
		)
	}
}

func ignored[T any](
	*Config[T],
) {
}

func positiveOr(
	value time.Duration,
	fallback time.Duration,
) time.Duration {
	if value <= 0 {
		return fallback
	}

	return value
}

// WithSyncProjections registers projections that run inside each event's
// transaction.
func WithSyncProjections[T any](
	p []SyncProjection[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.SyncProjections = p
	}
}

// WithDurableDelivery records a delivery entry for every event whose name
// match accepts and acknowledges it once handlers finish; see Processor.Recover.
func WithDurableDelivery[T any](
	match func(eventName string) bool,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.DurableMatch = match
	}
}

// WithSnapshotErrorHandler sets the callback for failed post-commit snapshots.
func WithSnapshotErrorHandler[T any](
	fn func(context.Context, asynxmd.Event[T], error),
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.SnapshotErrorHandler = fn
	}
}

func WithShards[T any](
	count int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		if count > 0 {
			cfg.Shards = count
		}
	}
}

func WithQueueDepth[T any](
	depth int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		if depth > 0 {
			cfg.QueueDepth = depth
		}
	}
}

func WithWorkersPerShard[T any](
	count int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		if count > 0 {
			cfg.WorkersPerShard = count
		}
	}
}

// WithPublishErrorHandler sets a callback invoked when Bus.PublishSync returns a
// non-nil error inside the dispatcher. When not set, publish errors are silently
// dropped.
func WithPublishErrorHandler[T any](
	fn asynxmd.PublishErrorHandler[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.PublishErrorHandler = fn
	}
}

// WithDispatcherFactory sets the factory that builds the dispatcher the
// processor delivers through, from the configuration with the defaults applied.
// A nil factory is ignored, leaving the in-memory default. When set, the
// factory runs even without a bus.
func WithDispatcherFactory[T any](
	f asynxmd.DispatcherFactory[T],
) Opt[T] {
	if f == nil {
		return ignored[T]
	}

	return func(
		cfg *Config[T],
	) {
		cfg.DispatcherFactory = f
	}
}

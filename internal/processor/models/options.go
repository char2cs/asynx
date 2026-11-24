package models

import (
	"context"
	"time"

	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"
)

// WithDurableSubscribers registers the durable subscribers the dispatcher runs,
// with retry, for durable deliveries.
func WithDurableSubscribers[T any](
	subs []subscription.Durable[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.DurableSubs = subs
	}
}

// WithDeliveryErrorHandler sets fn to observe every failed durable delivery
// attempt, including the ones that will be retried. It is called from the
// dispatcher's delivery goroutine and must not block for long; without it,
// failed attempts are retried silently up to the attempt cap and then parked
// or dead-lettered.
func WithDeliveryErrorHandler[T any](
	fn handler.DeliveryError[T],
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Dispatcher.OnDeliveryError = fn
	}
}

// WithDeliveryPanicHandler sets fn to be called when a durable handler panics.
// The panic is recovered either way and counts as a failed attempt; fn only
// observes it, from the dispatcher's delivery goroutine.
func WithDeliveryPanicHandler[T any](
	fn handler.Panic[T],
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

// WithDeadLetterHandler sets the handler that receives a delivery that ran out
// of attempts (see WithDeliveryMaxAttempts). When it accepts the delivery the
// delivery is acknowledged and the aggregate continues; without a handler the
// aggregate is parked until Resume.
func WithDeadLetterHandler[T any](
	fn handler.DeadLetter[T],
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

// WithSyncProjections registers projections that run, in order, inside the
// transaction that appends each matching event; one that fails rolls the
// command back and the command fails with ErrSyncProjection.
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
// Entries still unacknowledged after a crash are redelivered at-least-once by
// Recover. A nil match disables durable delivery, which is the default.
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
// A failed snapshot never fails the command, which is already committed;
// without fn the error is dropped and the aggregate is rebuilt from its stream.
func WithSnapshotErrorHandler[T any](
	fn func(context.Context, event.Event[T], error),
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.SnapshotErrorHandler = fn
	}
}

// WithShards sets the number of shards when count is positive; a non-positive
// count leaves the current value (default 8). Commands for one aggregate always
// land on the same shard, so more shards means more aggregates in parallel.
func WithShards[T any](
	count int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.Shards = positiveInt(
			count,
			cfg.Shards,
		)
	}
}

// WithQueueDepth sets the per-shard queue depth, which also bounds each
// aggregate's mailbox, when depth is positive; otherwise it is left unchanged
// and defaults to the worker count. Sends beyond it fail with ErrQueueFull.
func WithQueueDepth[T any](
	depth int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.QueueDepth = positiveInt(
			depth,
			cfg.QueueDepth,
		)
	}
}

// WithWorkersPerShard sets the workers per shard when count is positive; a
// non-positive count leaves the current value (default 8). Workers of a shard
// serve different aggregates in parallel, one command per aggregate at a time.
func WithWorkersPerShard[T any](
	count int,
) Opt[T] {
	return func(
		cfg *Config[T],
	) {
		cfg.WorkersPerShard = positiveInt(
			count,
			cfg.WorkersPerShard,
		)
	}
}

// WithPublishErrorHandler sets a callback invoked when Bus.PublishSync returns a
// non-nil error inside the dispatcher, and when Recover cannot rebuild an
// aggregate's pending event and parks the aggregate. When not set, those errors
// are dropped: the aggregate is still parked and observable through Paused and
// Inspect. A panic in fn is recovered and discarded, so a faulty hook can
// neither crash a delivery goroutine nor abort a recovery.
func WithPublishErrorHandler[T any](
	fn handler.PublishError[T],
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
	f dispatcher.Factory[T],
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

func positiveInt(
	value int,
	fallback int,
) int {
	if value > 0 {
		return value
	}

	return fallback
}

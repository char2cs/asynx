package models

import (
	"context"
	"time"
)

// ProjectionHandler is a subscription callback that receives the publishing
// context and the event that triggered it.
type ProjectionHandler[T any] func(
	context.Context,
	Event[T],
)

// PanicHandler is called when a ProjectionHandler panics.
// It receives the same context that was passed to Publish.
type PanicHandler[T any] func(
	context.Context,
	Event[T],
	any,
)

// TimeoutHandler is called when a ProjectionHandler exceeds its timeout.
// It receives the same context that was passed to Publish.
type TimeoutHandler[T any] func(
	context.Context,
	Event[T],
	time.Duration,
)

// PublishErrorHandler is called when Bus.Publish returns a non-nil error
// inside an async publish goroutine. The event has already been durably
// written to the event store; this callback is for observability only.
// When nil, publish errors are silently dropped.
type PublishErrorHandler[T any] func(
	context.Context,
	Event[T],
	error,
)

// ForgetHandler is called when an aggregate is forgotten.
// It receives the tombstone event; Event.Aggregate holds the aggregate's last known state.
type ForgetHandler[T any] func(context.Context, Event[T])

// DurableHandler is the callback of a durable subscriber (Builder.WithSubscriber).
// Unlike a ProjectionHandler it reports failure: a non-nil error, a panic or
// ErrHandlerTimeout-then-error leaves the delivery pending, holds back the
// aggregate's later events, and retries the handler with backoff until it
// succeeds, the attempt cap is reached (see WithDeliveryMaxAttempts: then the
// event is dead-lettered or the aggregate parked), or the process shuts down.
// Because delivery is at-least-once, a handler must be idempotent (dedupe on
// AggregateID and Version).
type DurableHandler[T any] func(context.Context, Event[T]) error

// DeliveryErrorHandler observes a failed durable delivery attempt. attempt
// counts from 1 for the event's current failing streak. Its error is a
// *SubscriberError naming the subscriber that failed; it matches ErrHandlerPanic
// or ErrHandlerTimeout for those failures, or wraps the handler's own error.
type DeliveryErrorHandler[T any] func(
	ctx context.Context,
	event Event[T],
	err error,
	attempt int,
)

// DeadLetterHandler is called when a durable delivery exhausted its attempts
// (Builder.WithDeliveryMaxAttempts). It receives the event and the errors of
// the last attempt: one *SubscriberError per subscriber that failed, joined
// (errors.As finds the first; unwrap with Unwrap() []error for each). Returning nil records the event as dealt with: the delivery is
// acknowledged, skipped, and the aggregate's later events flow again. Returning
// an error parks the aggregate instead (see ErrDeliveryParked).
type DeadLetterHandler[T any] func(context.Context, Event[T], error) error

// ForgetEventName is the EventName of the tombstone Forget writes. Every
// durable subscriber receives it, whatever its pattern, so it can drop
// per-aggregate state such as an (AggregateID, Version) dedupe table: a
// re-created aggregate restarts at version 1.
const ForgetEventName = "asynx.aggregate.forget"

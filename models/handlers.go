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
// succeeds. Because delivery is at-least-once, a handler must be idempotent
// (dedupe on AggregateID and Version).
type DurableHandler[T any] func(context.Context, Event[T]) error

// DeliveryErrorHandler observes a failed durable delivery attempt. attempt
// counts from 1 for the event's current failing streak. Its error matches
// ErrHandlerPanic or ErrHandlerTimeout for those failures, or is the handler's
// own error.
type DeliveryErrorHandler[T any] func(
	ctx context.Context,
	event Event[T],
	err error,
	attempt int,
)

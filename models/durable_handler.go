package models

import (
	"context"
)

// DurableHandler is the callback of a durable subscriber (see
// Builder.WithSubscriber). Unlike a ProjectionHandler it reports failure: a
// non-nil error, a panic, or a timeout followed by an error leaves the delivery
// pending, holds back the aggregate's later events, and retries the handler
// with backoff until it succeeds, the attempt cap is reached (see
// WithDeliveryMaxAttempts: then the event is dead-lettered or the aggregate
// parked), or the process shuts down.
//
// Because delivery is at-least-once, the same event can reach a handler more
// than once, after a retry or after a restart, so a handler must be idempotent:
// dedupe on the event's AggregateID and Version. Events of one aggregate reach
// a handler one at a time in version order; a handler that fails holds back
// every later event of that aggregate, and only that aggregate.
type DurableHandler[T any] func(
	context.Context,
	Event[T],
) error

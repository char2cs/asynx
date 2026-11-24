package models

import (
	"context"
)

// DurableHandler is the callback of a durable subscriber (Builder.WithSubscriber).
// Unlike a ProjectionHandler it reports failure: a non-nil error, a panic or
// ErrHandlerTimeout-then-error leaves the delivery pending, holds back the
// aggregate's later events, and retries the handler with backoff until it
// succeeds, the attempt cap is reached (see WithDeliveryMaxAttempts: then the
// event is dead-lettered or the aggregate parked), or the process shuts down.
// Because delivery is at-least-once, a handler must be idempotent (dedupe on
// AggregateID and Version).
type DurableHandler[T any] func(
	context.Context,
	Event[T],
) error

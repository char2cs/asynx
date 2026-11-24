package handler

import (
	"context"

	"github.com/char2cs/asynx/models/event"
)

// Projection is a live subscription callback. It receives the
// publishing context and the event that triggered it.
//
// It cannot report failure: it is fire-and-forget, run once per matching event
// on the bus, and is not retried. A panic is caught and reported to the bus's
// Panic (and runs the subscription's fallback, see subscription.WithFallback); one
// that runs past its timeout is reported to the Timeout. Handlers of
// one event run concurrently, but the dispatcher never starts an aggregate's
// next event before the previous one's handlers have finished. A handler that
// must not lose events, or must retry, is a durable subscriber instead (see
// Durable).
type Projection[T any] func(
	context.Context,
	event.Event[T],
)

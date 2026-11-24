package handler

import (
	"context"

	"github.com/char2cs/asynx/models/event"
)

// DeadLetter is called when a durable delivery exhausted its attempts
// (see Builder.WithDeliveryMaxAttempts). It receives the event and the errors
// of the last attempt: one *failure.SubscriberError per subscriber that failed, joined
// (errors.As finds the first; unwrap with Unwrap() []error for each).
//
// Returning nil records the event as dealt with: the delivery is acknowledged
// and skipped, and the aggregate's later events flow again. Returning an error
// parks the aggregate instead (see failure.ErrDeliveryParked). A panic in the handler
// is swallowed and counts as a refusal. A deletion marker, the tombstone of a
// Forget, is never dead-lettered.
type DeadLetter[T any] func(
	context.Context,
	event.Event[T],
	error,
) error

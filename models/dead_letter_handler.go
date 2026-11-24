package models

import (
	"context"
)

// DeadLetterHandler is called when a durable delivery exhausted its attempts
// (Builder.WithDeliveryMaxAttempts). It receives the event and the errors of
// the last attempt: one *SubscriberError per subscriber that failed, joined
// (errors.As finds the first; unwrap with Unwrap() []error for each). Returning nil records the event as dealt with: the delivery is
// acknowledged, skipped, and the aggregate's later events flow again. Returning
// an error parks the aggregate instead (see ErrDeliveryParked).
type DeadLetterHandler[T any] func(
	context.Context,
	Event[T],
	error,
) error

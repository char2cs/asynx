package models

import (
	"context"
)

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

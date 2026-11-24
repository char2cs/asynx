package reporter

import (
	"context"

	"github.com/char2cs/asynx/v2/models/handler"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/jobs"
)

// Reporter calls the configured error and panic handlers on behalf of the
// dispatcher. A nil handler makes the matching method a no-op.
//
// A handler that panics must not abandon a delivery, which would release the
// hold on the aggregate's later events, so its panic is recovered and reported
// to the panic handler as if a subscriber had panicked: PublishError and
// DeliveryError forward the panic of their handler to Panic. The panic handler
// is the last hook there is, so a panic in the panic handler itself has nowhere
// left to go: Panic recovers it and drops it, and that is its documented
// contract.
type Reporter[T any] interface {
	// PublishError reports an error from publishing job's event to the live
	// subscribers.
	PublishError(
		ctx context.Context,
		job jobs.Job[T],
		err error,
	)

	// DeliveryError reports a failed durable delivery attempt of job's event.
	// attempt counts from 1.
	DeliveryError(
		ctx context.Context,
		job jobs.Job[T],
		err error,
		attempt int,
	)

	// Panic reports the value recovered from a handler that panicked while
	// handling job's event.
	Panic(
		ctx context.Context,
		job jobs.Job[T],
		recovered any,
	)
}

// New creates a Reporter that calls the given handlers; any of them may be nil.
func New[T any](
	onPublishError handler.PublishError[T],
	onDeliveryError handler.DeliveryError[T],
	onPanic handler.Panic[T],
) Reporter[T] {
	return &handlerReporter[T]{
		onPublishError:  onPublishError,
		onDeliveryError: onDeliveryError,
		onPanic:         onPanic,
	}
}

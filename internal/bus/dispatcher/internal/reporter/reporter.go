// Package reporter delivers the dispatcher's observability callbacks without
// letting a misbehaving callback disturb a delivery.
package reporter

import (
	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

// Reporter calls the configured error and panic handlers on behalf of the
// dispatcher. A handler that panics is swallowed, because it must not take down
// a worker or abandon a delivery and release the hold on the aggregate's later
// events. A nil handler makes the matching method a no-op.
type Reporter[T any] interface {
	// PublishError reports an error from publishing job's event to the live
	// subscribers.
	PublishError(
		job jobs.Job[T],
		err error,
	)

	// DeliveryError reports a failed durable delivery attempt of job's event.
	// attempt counts from 1.
	DeliveryError(
		job jobs.Job[T],
		err error,
		attempt int,
	)

	// Panic reports the value recovered from a handler that panicked while
	// handling job's event.
	Panic(
		job jobs.Job[T],
		recovered any,
	)
}

// New creates a Reporter that calls the given handlers; any of them may be nil.
func New[T any](
	onPublishError asynxmd.PublishErrorHandler[T],
	onDeliveryError asynxmd.DeliveryErrorHandler[T],
	onPanic asynxmd.PanicHandler[T],
) Reporter[T] {
	return &handlerReporter[T]{
		onPublishError:  onPublishError,
		onDeliveryError: onDeliveryError,
		onPanic:         onPanic,
	}
}

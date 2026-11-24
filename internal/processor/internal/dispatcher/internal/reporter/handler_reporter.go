package reporter

import (
	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

type handlerReporter[T any] struct {
	onPublishError  asynxmd.PublishErrorHandler[T]
	onDeliveryError asynxmd.DeliveryErrorHandler[T]
	onPanic         asynxmd.PanicHandler[T]
}

// PublishError calls the publish error handler, if any, swallowing its panic.
func (r *handlerReporter[T]) PublishError(
	job contracts.Job[T],
	err error,
) {
	if r.onPublishError == nil {
		return
	}

	defer swallowPanic()
	r.onPublishError(
		job.Context(),
		job.Event(),
		err,
	)
}

// DeliveryError calls the delivery error handler, if any, swallowing its panic.
func (r *handlerReporter[T]) DeliveryError(
	job contracts.Job[T],
	err error,
	attempt int,
) {
	if r.onDeliveryError == nil {
		return
	}

	defer swallowPanic()
	r.onDeliveryError(
		job.Context(),
		job.Event(),
		err,
		attempt,
	)
}

// Panic calls the panic handler, if any, swallowing its panic.
func (r *handlerReporter[T]) Panic(
	job contracts.Job[T],
	recovered any,
) {
	if r.onPanic == nil {
		return
	}

	defer swallowPanic()
	r.onPanic(
		job.Context(),
		job.Event(),
		recovered,
	)
}

func swallowPanic() {
	recovered := recover()
	_ = recovered
}

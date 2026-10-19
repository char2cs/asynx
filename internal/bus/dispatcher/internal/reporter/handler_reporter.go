package reporter

import (
	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type handlerReporter[T any] struct {
	onPublishError  asynxmd.PublishErrorHandler[T]
	onDeliveryError asynxmd.DeliveryErrorHandler[T]
	onPanic         asynxmd.PanicHandler[T]
}

func (r *handlerReporter[T]) PublishError(
	job jobs.Job[T],
	err error,
) {
	if r.onPublishError == nil {
		return
	}
	defer func() { _ = recover() }()
	r.onPublishError(job.Context(), job.Event(), err)
}

func (r *handlerReporter[T]) DeliveryError(
	job jobs.Job[T],
	err error,
	attempt int,
) {
	if r.onDeliveryError == nil {
		return
	}
	defer func() { _ = recover() }()
	r.onDeliveryError(job.Context(), job.Event(), err, attempt)
}

func (r *handlerReporter[T]) Panic(
	job jobs.Job[T],
	recovered any,
) {
	if r.onPanic == nil {
		return
	}
	defer func() { _ = recover() }()
	r.onPanic(job.Context(), job.Event(), recovered)
}

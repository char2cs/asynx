package reporter

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

type handlerReporter[T any] struct {
	onPublishError  asynxmd.PublishErrorHandler[T]
	onDeliveryError asynxmd.DeliveryErrorHandler[T]
	onPanic         asynxmd.PanicHandler[T]
}

// PublishError calls the publish error handler, if any, with the event of job
// and err. A panic in the handler is recovered and reported to the panic
// handler, so it never reaches the delivery that is reporting.
func (r *handlerReporter[T]) PublishError(
	ctx context.Context,
	job contracts.Job[T],
	err error,
) {
	if r.onPublishError == nil {
		return
	}

	defer r.forwardPanic(
		ctx,
		job,
	)
	r.onPublishError(
		ctx,
		job.Event(),
		err,
	)
}

// DeliveryError calls the delivery error handler, if any, with the event of
// job, err and the attempt number. A panic in the handler is recovered and
// reported to the panic handler, so it never reaches the delivery that is
// reporting.
func (r *handlerReporter[T]) DeliveryError(
	ctx context.Context,
	job contracts.Job[T],
	err error,
	attempt int,
) {
	if r.onDeliveryError == nil {
		return
	}

	defer r.forwardPanic(
		ctx,
		job,
	)
	r.onDeliveryError(
		ctx,
		job.Event(),
		err,
		attempt,
	)
}

// Panic calls the panic handler, if any, with the event of job and the
// recovered value. A panic in the panic handler itself is recovered and
// dropped: it is the last hook there is, so there is nowhere left to report
// that panic to, and letting it escape would abandon the delivery.
func (r *handlerReporter[T]) Panic(
	ctx context.Context,
	job contracts.Job[T],
	recovered any,
) {
	if r.onPanic == nil {
		return
	}

	defer dropPanicHandlerPanic()
	r.onPanic(
		ctx,
		job.Event(),
		recovered,
	)
}

func (r *handlerReporter[T]) forwardPanic(
	ctx context.Context,
	job contracts.Job[T],
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	r.Panic(
		ctx,
		job,
		recovered,
	)
}

func dropPanicHandlerPanic() {
	if recover() != nil {
		return
	}
}

package reporter

import (
	"context"
	"fmt"
	"os"

	"github.com/char2cs/asynx/v2/models/handler"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/contracts"
)

type handlerReporter[T any] struct {
	onPublishError  handler.PublishError[T]
	onDeliveryError handler.DeliveryError[T]
	onPanic         handler.Panic[T]
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
// written to stderr as one line: it is the last hook there is, so stderr is the
// only place left to report it, and letting it escape would abandon the
// delivery.
func (r *handlerReporter[T]) Panic(
	ctx context.Context,
	job contracts.Job[T],
	recovered any,
) {
	if r.onPanic == nil {
		return
	}

	defer writePanicHandlerPanicToStderr()
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

func writePanicHandlerPanicToStderr() {
	recovered := recover()
	if recovered == nil {
		return
	}

	fmt.Fprintf(
		os.Stderr,
		"asynx: the panic handler panicked and was contained: %v\n",
		recovered,
	)
}

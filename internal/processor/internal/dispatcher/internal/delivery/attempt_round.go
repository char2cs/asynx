package delivery

import (
	"context"
	"errors"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

type attemptRound[T any] struct {
	deliverer *durableDeliverer[T]
	job       contracts.Job[T]
	attempt   int
	failed    []asynxmd.DurableSub[T]
	errs      []error
}

func (r *attemptRound[T]) run(
	ctx context.Context,
	pending []asynxmd.DurableSub[T],
) {
	for _, sub := range pending {
		r.deliverTo(
			ctx,
			sub,
		)
	}
}

func (r *attemptRound[T]) deliverTo(
	ctx context.Context,
	sub asynxmd.DurableSub[T],
) {
	call := &subscriberCall[T]{
		deliverer: r.deliverer,
		job:       r.job,
		sub:       sub,
		attempt:   r.attempt,
	}
	err := call.run(ctx)
	if err == nil {
		return
	}

	failure := &asynxmd.SubscriberError{Subscriber: sub.Name, Err: err}
	r.deliverer.reports.DeliveryError(
		ctx,
		r.job,
		failure,
		r.attempt,
	)
	r.failed = append(
		r.failed,
		sub,
	)
	r.errs = append(
		r.errs,
		failure,
	)
}

func (r *attemptRound[T]) cause() error {
	return errors.Join(r.errs...)
}

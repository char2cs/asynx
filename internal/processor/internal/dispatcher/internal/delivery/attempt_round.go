package delivery

import (
	"context"
	"errors"

	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/subscription"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/contracts"
)

type attemptRound[T any] struct {
	deliverer *durableDeliverer[T]
	job       contracts.Job[T]
	attempt   int
	failed    []subscription.Durable[T]
	errs      []error
}

func (r *attemptRound[T]) run(
	ctx context.Context,
	pending []subscription.Durable[T],
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
	sub subscription.Durable[T],
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

	failed := &failure.SubscriberError{Subscriber: sub.Name, Err: err}
	r.deliverer.reports.DeliveryError(
		ctx,
		r.job,
		failed,
		r.attempt,
	)
	r.failed = append(
		r.failed,
		sub,
	)
	r.errs = append(
		r.errs,
		failed,
	)
}

func (r *attemptRound[T]) cause() error {
	return errors.Join(r.errs...)
}

package delivery

import (
	"context"
	"errors"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
)

const backoffFactor = 2

var errRetrying = errors.New("delivery attempt failed, retrying")

type retryLoop[T any] struct {
	deliverer   *durableDeliverer[T]
	aggregateID string
	halt        <-chan struct{}
	job         contracts.Job[T]
	pending     []asynxmd.DurableSub[T]
	attempt     int
	backoff     time.Duration
}

func (l *retryLoop[T]) run(
	ctx context.Context,
) error {
	err := l.step(ctx)
	for errors.Is(
		err,
		errRetrying,
	) {
		err = l.step(ctx)
	}

	return err
}

func (l *retryLoop[T]) step(
	ctx context.Context,
) error {
	failed, last := l.deliverer.attempt(
		ctx,
		l.job,
		l.pending,
		l.attempt,
	)
	l.settleAfterFirst()
	l.pending = failed
	if len(failed) == 0 {
		return nil
	}

	if l.attempt >= l.deliverer.maxAttempts {
		return l.deliverer.giveUp(
			ctx,
			l.aggregateID,
			l.halt,
			l.job,
			last,
			l.attempt,
		)
	}

	return l.retry(ctx)
}

func (l *retryLoop[T]) retry(
	ctx context.Context,
) error {
	err := l.deliverer.wait(
		ctx,
		l.aggregateID,
		l.halt,
		l.job,
		l.backoff,
	)
	if err != nil {
		return err
	}

	l.attempt++
	l.backoff = min(
		l.backoff*backoffFactor,
		l.deliverer.retryMax,
	)
	return errRetrying
}

func (l *retryLoop[T]) settleAfterFirst() {
	if l.attempt != 1 {
		return
	}

	l.job.MarkSettled()
}

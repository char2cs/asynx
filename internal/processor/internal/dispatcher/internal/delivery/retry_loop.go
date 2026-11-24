package delivery

import (
	"context"
	"time"

	"github.com/char2cs/asynx/v2/models/subscription"

	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/contracts"
)

const backoffFactor = 2

type retryLoop[T any] struct {
	deliverer   *durableDeliverer[T]
	aggregateID string
	halt        <-chan struct{}
	job         contracts.Job[T]
	pending     []subscription.Durable[T]
	attempt     int
	backoff     time.Duration
	last        error
	stopped     error
}

func (l *retryLoop[T]) run(
	ctx context.Context,
) error {
	l.attemptPending(ctx)
	for l.retriable() {
		l.backOffAndRetry(ctx)
	}

	return l.conclude(ctx)
}

func (l *retryLoop[T]) retriable() bool {
	return l.stopped == nil &&
		len(l.pending) > 0 &&
		l.attempt < l.deliverer.maxAttempts
}

func (l *retryLoop[T]) backOffAndRetry(
	ctx context.Context,
) {
	err := l.deliverer.wait(
		ctx,
		l.aggregateID,
		l.halt,
		l.job,
		l.backoff,
	)
	if err != nil {
		l.stopped = err
		return
	}

	l.advance()
	l.attemptPending(ctx)
}

func (l *retryLoop[T]) conclude(
	ctx context.Context,
) error {
	if l.stopped != nil {
		return l.stopped
	}

	if len(l.pending) == 0 {
		return nil
	}

	return l.deliverer.giveUp(
		ctx,
		l.aggregateID,
		l.halt,
		l.job,
		l.last,
		l.attempt,
	)
}

func (l *retryLoop[T]) attemptPending(
	ctx context.Context,
) {
	round := &attemptRound[T]{
		deliverer: l.deliverer,
		job:       l.job,
		attempt:   l.attempt,
	}
	round.run(
		ctx,
		l.pending,
	)
	l.pending = round.failed
	l.last = round.cause()
	l.settleAfterFirst()
}

func (l *retryLoop[T]) advance() {
	l.attempt++
	l.backoff = min(
		l.backoff*backoffFactor,
		l.deliverer.retryMax,
	)
}

func (l *retryLoop[T]) settleAfterFirst() {
	if l.attempt != 1 {
		return
	}

	l.job.MarkSettled()
}

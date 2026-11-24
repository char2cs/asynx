package delivery

import (
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

func (l *retryLoop[T]) run() error {
	err := l.step()
	for errors.Is(
		err,
		errRetrying,
	) {
		err = l.step()
	}

	return err
}

func (l *retryLoop[T]) step() error {
	failed, last := l.deliverer.attempt(
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
			l.aggregateID,
			l.halt,
			l.job,
			last,
			l.attempt,
		)
	}

	err := l.deliverer.wait(
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

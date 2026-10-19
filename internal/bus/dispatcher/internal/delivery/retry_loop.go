package delivery

import (
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type retryLoop[T any] struct {
	deliverer   *durableDeliverer[T]
	aggregateID string
	halt        <-chan struct{}
	job         jobs.Job[T]
	pending     []asynxmd.DurableSub[T]
	attempt     int
	backoff     time.Duration
	err         error
}

func (l *retryLoop[T]) step() bool {
	failed, last := l.deliverer.attempt(l.job, l.pending, l.attempt)
	l.settleAfterFirst()
	l.pending = failed
	if len(failed) == 0 {
		return false
	}
	if l.attempt >= l.deliverer.maxAttempts {
		l.err = l.deliverer.giveUp(l.aggregateID, l.halt, l.job, last, l.attempt)
		return false
	}
	l.err = l.deliverer.wait(l.aggregateID, l.halt, l.job, l.backoff)
	if l.err != nil {
		return false
	}
	l.attempt++
	l.backoff = min(l.backoff*2, l.deliverer.retryMax)
	return true
}

func (l *retryLoop[T]) settleAfterFirst() {
	if l.attempt != 1 {
		return
	}
	l.job.MarkSettled()
}

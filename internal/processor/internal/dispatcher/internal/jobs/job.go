package jobs

import (
	"context"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/event"
)

// Job is one event on its way through the dispatcher; see contracts.Job.
type Job[T any] = contracts.Job[T]

// New creates a Job for request. cancel releases the context the job is
// delivered under, which travels beside the job and never in it, and is called
// when the job completes.
func New[T any](
	request dispatch.Request[T],
	cancel context.CancelFunc,
) Job[T] {
	return &dispatchJob[T]{
		event:    request.Event,
		cancel:   cancel,
		ack:      request.Ack,
		forget:   isForget(request),
		skipLive: request.Kind == dispatch.KindRedelivery,
		done:     make(chan struct{}),
		final:    make(chan struct{}),
	}
}

func isForget[T any](
	request dispatch.Request[T],
) bool {
	if request.Kind == dispatch.KindTombstone {
		return true
	}

	return request.Kind == dispatch.KindRedelivery &&
		request.Event.EventName == event.ForgetEventName
}

package jobs

import (
	"context"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/contracts"
	asynxmd "github.com/char2cs/asynx/models"
)

// Job is one event on its way through the dispatcher; see contracts.Job.
type Job[T any] = contracts.Job[T]

// New creates a Job for request. cancel releases the context the job is
// delivered under, which travels beside the job and never in it, and is called
// when the job completes.
func New[T any](
	request asynxmd.DispatchRequest[T],
	cancel context.CancelFunc,
) Job[T] {
	return &dispatchJob[T]{
		event:    request.Event,
		cancel:   cancel,
		ack:      request.Ack,
		forget:   isForget(request),
		skipLive: request.Kind == asynxmd.DispatchRedelivery,
		done:     make(chan struct{}),
		final:    make(chan struct{}),
	}
}

func isForget[T any](
	request asynxmd.DispatchRequest[T],
) bool {
	if request.Kind == asynxmd.DispatchTombstone {
		return true
	}

	return request.Kind == asynxmd.DispatchRedelivery &&
		request.Event.EventName == asynxmd.ForgetEventName
}

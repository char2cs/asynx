// Package jobs holds the unit of work the default dispatcher moves through its
// queues: one event, its delivery context and its completion state.
package jobs

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

// Job is one event on its way through the dispatcher.
//
// A job settles once, when its handlers have run at least once or it is
// declared unrun, and completes once, when its delivery is over. Both
// transitions are idempotent and safe for concurrent use.
type Job[T any] interface {
	// AggregateID returns the aggregate the event belongs to.
	AggregateID() string

	// Version returns the event's version within its aggregate.
	Version() int64

	// Event returns the event being delivered.
	Event() asynxmd.Event[T]

	// Context returns the context handlers of this job run with. Completing
	// the job cancels it.
	Context() context.Context

	// Durable reports whether the job runs durable subscribers: it carries an
	// Ack or it is a deletion marker.
	Durable() bool

	// Acked reports whether the job carries an Ack. Only such a job is held
	// back by a park: refused while its aggregate is parked, dropped from the
	// queue when a park is imposed, and parking its aggregate when it gives
	// up. A deletion marker or redelivery with a nil Ack is durable but not
	// acked.
	Acked() bool

	// Forget reports whether the job is a deletion marker, which every durable
	// subscriber receives whatever its pattern.
	Forget() bool

	// SkipLive reports whether the live subscribers are kept out of the job,
	// as in a redelivery.
	SkipLive() bool

	// Ack returns the function to call once every durable subscriber has
	// succeeded, or nil when the job is live only.
	Ack() func(context.Context) error

	// Ticket returns the ticket that observes the job's progress.
	Ticket() asynxmd.Ticket

	// MarkSettled records that the handlers have run at least once. Only the
	// first settle of a job takes effect.
	MarkSettled()

	// MarkUnrun settles a job whose handlers never ran and records why. It has
	// no effect on a job that already settled.
	MarkUnrun(err error)

	// Complete ends the job's delivery with err: it cancels the job's context,
	// closes the ticket's Final channel and runs the completion callback. Only
	// the first call takes effect.
	Complete(err error)

	// OnComplete sets the callback Complete runs once, replacing any previous
	// one. Set it before the job is shared between goroutines.
	OnComplete(callback func())
}

// New creates a Job for request, delivered under ctx. cancel releases ctx and
// is called when the job completes.
func New[T any](
	request asynxmd.DispatchRequest[T],
	ctx context.Context,
	cancel context.CancelFunc,
) Job[T] {
	return &dispatchJob[T]{
		event:    request.Event,
		ctx:      ctx,
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

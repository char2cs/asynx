package jobs

import (
	"context"
	"sync"

	asynxmd "github.com/char2cs/asynx/models"
)

type dispatchJob[T any] struct {
	event      asynxmd.Event[T]
	cancel     context.CancelFunc
	ack        func(context.Context) error
	forget     bool
	skipLive   bool
	done       chan struct{}
	settle     sync.Once
	unrun      error
	final      chan struct{}
	finish     sync.Once
	err        error
	onComplete func()
}

// AggregateID returns the aggregate the event belongs to, the key under which
// the registry queues the job and chains it behind that aggregate's earlier
// jobs.
func (j *dispatchJob[T]) AggregateID() string {
	return j.event.AggregateID
}

// Version returns the event's version within its aggregate: with AggregateID,
// the key under which the tracker finds the job's ticket and a park records
// the job as undelivered.
func (j *dispatchJob[T]) Version() int64 {
	return j.event.Version
}

// Event returns the event being delivered, which every handler of the job
// receives unchanged.
func (j *dispatchJob[T]) Event() asynxmd.Event[T] {
	return j.event
}

// Durable reports whether the job runs durable subscribers: it carries an Ack
// or it is a deletion marker. A job that is not durable is delivered to the
// live subscribers only and ends once they have been published to.
func (j *dispatchJob[T]) Durable() bool {
	return j.ack != nil || j.forget
}

// Acked reports whether the job carries an Ack. Only such a job is held back
// by a park: refused while its aggregate is parked, dropped from the queue
// when a park is imposed, and parking its aggregate when its delivery gives
// up. A deletion marker or redelivery with a nil Ack is durable but not
// acked.
func (j *dispatchJob[T]) Acked() bool {
	return j.ack != nil
}

// Forget reports whether the job is a deletion marker, a tombstone or a
// redelivered event named ForgetEventName, which every durable subscriber
// receives whatever its pattern and which is never dead-lettered.
func (j *dispatchJob[T]) Forget() bool {
	return j.forget
}

// SkipLive reports whether the live subscribers are kept out of the job, as in
// a redelivery: they received the event when it was first dispatched.
func (j *dispatchJob[T]) SkipLive() bool {
	return j.skipLive
}

// Ack returns the function to call once every durable subscriber has
// succeeded, or nil for a job that is live only or a deletion marker without
// one. The caller runs it under the job's own context.
func (j *dispatchJob[T]) Ack() func(context.Context) error {
	return j.ack
}

// Ticket returns the ticket that observes the job's progress: Settled closes
// once the handlers have run at least once or the job was declared unrun,
// Final closes when the delivery is over, and Err and Unrun report how it
// ended once the matching channel is closed. Every call returns a ticket over
// the same channels.
func (j *dispatchJob[T]) Ticket() asynxmd.Ticket {
	return asynxmd.Ticket{
		Settled: j.done,
		Final:   j.final,
		Err:     func() error { return j.err },
		Unrun:   func() error { return j.unrun },
	}
}

// MarkSettled records that the handlers have run at least once and closes the
// ticket's Settled channel. Only the first settle of a job, by this or
// MarkUnrun, takes effect.
func (j *dispatchJob[T]) MarkSettled() {
	j.settle.Do(func() { close(j.done) })
}

// MarkUnrun settles a job whose handlers never ran, records err as the reason
// its ticket's Unrun reports and closes the Settled channel. It has no effect
// on a job that already settled. The registry calls it under its mutex, so it
// never calls back into the registry.
func (j *dispatchJob[T]) MarkUnrun(
	err error,
) {
	j.settle.Do(func() {
		j.unrun = err
		close(j.done)
	})
}

// Complete ends the job's delivery with err: it records err for the ticket's
// Err, cancels the context the job is delivered under, closes the ticket's
// Final channel and runs the completion callback. Only the first call takes
// effect, and it may run under the registry's mutex, so neither it nor the
// callback calls back into the registry.
func (j *dispatchJob[T]) Complete(
	err error,
) {
	j.finish.Do(func() { j.finalize(err) })
}

func (j *dispatchJob[T]) finalize(
	err error,
) {
	j.err = err
	j.cancel()
	close(j.final)
	if j.onComplete == nil {
		return
	}

	j.onComplete()
}

// OnComplete sets the callback Complete runs once, replacing any previous one.
// The tracker sets it, before the job is shared between goroutines, to remove
// the job from its books when the delivery is over.
func (j *dispatchJob[T]) OnComplete(
	callback func(),
) {
	j.onComplete = callback
}

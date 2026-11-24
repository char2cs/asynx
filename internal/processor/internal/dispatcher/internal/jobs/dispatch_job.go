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

// AggregateID returns the aggregate the event belongs to.
func (j *dispatchJob[T]) AggregateID() string {
	return j.event.AggregateID
}

// Version returns the event's version within its aggregate.
func (j *dispatchJob[T]) Version() int64 {
	return j.event.Version
}

// Event returns the event being delivered.
func (j *dispatchJob[T]) Event() asynxmd.Event[T] {
	return j.event
}

// Durable reports whether the job runs durable subscribers.
func (j *dispatchJob[T]) Durable() bool {
	return j.ack != nil || j.forget
}

// Acked reports whether the job carries an Ack.
func (j *dispatchJob[T]) Acked() bool {
	return j.ack != nil
}

// Forget reports whether the job is a deletion marker.
func (j *dispatchJob[T]) Forget() bool {
	return j.forget
}

// SkipLive reports whether the live subscribers are kept out of the job.
func (j *dispatchJob[T]) SkipLive() bool {
	return j.skipLive
}

// Ack returns the acknowledgement function, nil for a live-only job.
func (j *dispatchJob[T]) Ack() func(context.Context) error {
	return j.ack
}

// Ticket returns the ticket that observes the job's progress.
func (j *dispatchJob[T]) Ticket() asynxmd.Ticket {
	return asynxmd.Ticket{
		Settled: j.done,
		Final:   j.final,
		Err:     func() error { return j.err },
		Unrun:   func() error { return j.unrun },
	}
}

// MarkSettled records that the handlers have run at least once.
func (j *dispatchJob[T]) MarkSettled() {
	j.settle.Do(func() { close(j.done) })
}

// MarkUnrun settles a job whose handlers never ran and records why.
func (j *dispatchJob[T]) MarkUnrun(
	err error,
) {
	j.settle.Do(func() {
		j.unrun = err
		close(j.done)
	})
}

// Complete ends the job's delivery with err; only the first call takes effect.
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

// OnComplete sets the callback Complete runs once.
func (j *dispatchJob[T]) OnComplete(
	callback func(),
) {
	j.onComplete = callback
}

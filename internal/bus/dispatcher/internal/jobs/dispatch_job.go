package jobs

import (
	"context"
	"sync"

	asynxmd "github.com/char2cs/asynx/models"
)

type dispatchJob[T any] struct {
	event      asynxmd.Event[T]
	ctx        context.Context
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

func (j *dispatchJob[T]) AggregateID() string {
	return j.event.AggregateID
}

func (j *dispatchJob[T]) Version() int64 {
	return j.event.Version
}

func (j *dispatchJob[T]) Event() asynxmd.Event[T] {
	return j.event
}

func (j *dispatchJob[T]) Context() context.Context {
	return j.ctx
}

func (j *dispatchJob[T]) Durable() bool {
	return j.ack != nil || j.forget
}

func (j *dispatchJob[T]) Forget() bool {
	return j.forget
}

func (j *dispatchJob[T]) SkipLive() bool {
	return j.skipLive
}

func (j *dispatchJob[T]) Ack() func(context.Context) error {
	return j.ack
}

func (j *dispatchJob[T]) Ticket() asynxmd.Ticket {
	return asynxmd.Ticket{
		Settled: j.done,
		Final:   j.final,
		Err:     func() error { return j.err },
		Unrun:   func() error { return j.unrun },
	}
}

func (j *dispatchJob[T]) MarkSettled() {
	j.settle.Do(func() { close(j.done) })
}

func (j *dispatchJob[T]) MarkUnrun(
	err error,
) {
	j.settle.Do(func() {
		j.unrun = err
		close(j.done)
	})
}

func (j *dispatchJob[T]) Complete(
	err error,
) {
	j.finish.Do(func() {
		j.err = err
		j.cancel()
		close(j.final)
		if j.onComplete == nil {
			return
		}
		j.onComplete()
	})
}

func (j *dispatchJob[T]) OnComplete(
	callback func(),
) {
	j.onComplete = callback
}

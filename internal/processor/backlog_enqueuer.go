package processor

import (
	"context"

	asynxmd "github.com/char2cs/asynx/models"
)

type backlogEnqueuer[T any] struct {
	dispatcher asynxmd.Dispatcher[T]
	store      asynxmd.Store
	aggregate  string
	vanished   map[int64]struct{}
	err        error
}

func (e *backlogEnqueuer[T]) enqueue(
	ctx context.Context,
	evt asynxmd.Event[T],
) {
	if _, wanted := e.vanished[evt.Version]; !wanted || e.err != nil {
		return
	}
	_, e.err = e.dispatcher.Enqueue(ctx, asynxmd.DispatchRequest[T]{
		Event: evt,
		Ack:   e.ack(evt.Version),
		Kind:  asynxmd.DispatchRedelivery,
	})
	if e.err != nil {
		return
	}
	delete(e.vanished, evt.Version)
}

func (e *backlogEnqueuer[T]) ack(
	version int64,
) func(context.Context) error {
	return func(ctx context.Context) error {
		return e.store.AckDelivery(ctx, e.aggregate, version)
	}
}

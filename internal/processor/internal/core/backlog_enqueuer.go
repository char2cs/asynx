package core

import (
	"context"

	"github.com/char2cs/asynx/models/dispatch"
	"github.com/char2cs/asynx/models/dispatcher"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

type backlogEnqueuer[T any] struct {
	dispatcher dispatcher.Dispatcher[T]
	store      storage.Store
	aggregate  string
	vanished   map[int64]struct{}
	err        error
}

func (e *backlogEnqueuer[T]) enqueue(
	ctx context.Context,
	evt event.Event[T],
) {
	if _, wanted := e.vanished[evt.Version]; !wanted ||
		e.err != nil {
		return
	}

	_, e.err = e.dispatcher.Enqueue(
		ctx,
		dispatch.Request[T]{
			Event: evt,
			Ack:   e.ack(evt.Version),
			Kind:  dispatch.KindRedelivery,
		},
	)
	if e.err != nil {
		return
	}

	delete(
		e.vanished,
		evt.Version,
	)
}

func (e *backlogEnqueuer[T]) ack(
	version int64,
) func(context.Context) error {
	return func(
		ctx context.Context,
	) error {
		return e.store.AckDelivery(
			ctx,
			e.aggregate,
			version,
		)
	}
}

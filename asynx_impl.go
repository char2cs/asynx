package asynx

import (
	"context"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/models"
)

type asynxImpl[T any] struct {
	proc processor.Processor[T]
	es   eventstore.EventStore[T]
	bus  models.Bus[T]
}

func (i *asynxImpl[T]) Send(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.Send(
		ctx,
		cmd,
	)
}

func (i *asynxImpl[T]) SendWait(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.SendWait(
		ctx,
		cmd,
	)
}

func (i *asynxImpl[T]) Shutdown(
	ctx context.Context,
) error {
	return i.proc.Shutdown(ctx)
}

func (i *asynxImpl[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	return i.es.Get(
		ctx,
		aggregateID,
	)
}

func (i *asynxImpl[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return i.es.Exists(
		ctx,
		aggregateID,
	)
}

func (i *asynxImpl[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return i.es.Preload(
		ctx,
		aggregateID,
	)
}

func (i *asynxImpl[T]) Subscribe(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) (string, error) {
	return i.bus.Subscribe(
		pattern,
		handler,
		opts...,
	)
}

func (i *asynxImpl[T]) Unsubscribe(
	id string,
) error {
	return i.bus.Unsubscribe(id)
}

func (i *asynxImpl[T]) Listen(
	pattern string,
	count int,
) (<-chan models.Event[T], func(), error) {
	if pattern == "" {
		return nil,
			nil,
			models.ErrEmptyPattern
	}

	l := newListener(
		i.bus,
		count,
	)
	id, err := i.bus.Subscribe(
		Topic(pattern),
		l.handle,
	)
	if err != nil {
		return nil,
			nil,
			err
	}

	l.publishSubscription(id)

	return l.ch,
		l.unsubscribe,
		nil
}

func (i *asynxImpl[T]) SubscribeWait(
	ctx context.Context,
	pattern string,
) (models.Event[T], error) {
	ch, unsub, err := i.Listen(
		pattern,
		1,
	)
	if err != nil {
		var zero models.Event[T]
		return zero,
			err
	}

	defer unsub()

	select {
	case evt := <-ch:
		return evt,
			nil
	case <-ctx.Done():
		var zero models.Event[T]
		return zero,
			ctx.Err()
	}
}

func (i *asynxImpl[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn models.ProjectionHandler[T],
) error {
	return i.es.Replay(
		ctx,
		aggregateID,
		fromVersion,
		toVersion,
		fn,
	)
}

// WaitPublish blocks until all async event publishes complete.
// Only for use in tests; do not call in production code.
func (i *asynxImpl[T]) WaitPublish() {
	i.proc.WaitPublish()
	i.bus.WaitForHandlers()
}

func (i *asynxImpl[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	return i.proc.Forget(
		ctx,
		aggregateID,
	)
}

func (i *asynxImpl[T]) PausedAggregates() []string {
	return i.proc.Paused()
}

func (i *asynxImpl[T]) InspectDelivery(
	aggregateID string,
) models.DeliveryStatus {
	return i.proc.Inspect(aggregateID)
}

func (i *asynxImpl[T]) ResumeDeliveries(
	ctx context.Context,
) error {
	return i.proc.Resume(
		ctx,
		recoveryBatch,
	)
}

// OnForget registers fn to run when an aggregate is forgotten. The dots of the
// subscription pattern are escaped so the bus treats it as a literal, not a
// wildcard.
func (i *asynxImpl[T]) OnForget(
	fn models.ForgetHandler[T],
) (string, error) {
	return i.bus.Subscribe(
		"asynx\\.aggregate\\.forget",
		models.ProjectionHandler[T](fn),
	)
}

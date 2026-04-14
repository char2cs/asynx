package asynx

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/models"
)

type Asynx[T any] interface {
	// Send validates and persists the command, then returns the resulting Event[T]
	// once it is durably written. Projection handlers are fired asynchronously —
	// when Send returns, handlers may not have run yet.
	// Use SendWait when the caller needs projections to be up to date before proceeding.
	Send(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// SendWait behaves like Send but additionally blocks until all matching
	// projection handlers have completed. When SendWait returns without error,
	// the event is persisted and every projection subscribed to it has finished.
	// Use Send when projection consistency is not required at the call site.
	SendWait(
		ctx context.Context,
		cmd models.Command[T],
	) (models.Event[T], error)

	// Forget writes a tombstone event for the aggregate, notifies all ForgetHandlers
	// synchronously, then erases all events, snapshots, and cached state.
	// Returns ErrValidation if the aggregate does not exist.
	Forget(
		ctx context.Context,
		aggregateID string,
	) error

	// OnForget registers a handler invoked when any aggregate is forgotten.
	// The handler receives the tombstone event; Event.Aggregate holds the last known state.
	// Returns a subscription ID that can be passed to Unsubscribe.
	OnForget(
		fn models.ForgetHandler[T],
	) (string, error)

	Shutdown(
		ctx context.Context,
	) error

	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)
	Preload(
		ctx context.Context,
		aggregateID string,
	) error

	Subscribe(
		pattern string,
		handler models.ProjectionHandler[T],
		opts ...models.SubscriptionOpt[T],
	) (string, error)
	Unsubscribe(
		id string,
	) error

	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn models.ProjectionHandler[T],
	) error

	WaitPublish()
}

type asynxImpl[T any] struct {
	proc *processor.Processor[T]
	es   *eventstore.EventStore[T]
	bus  models.Bus[T]
}

func (i *asynxImpl[T]) Send(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.Send(ctx, cmd)
}

func (i *asynxImpl[T]) SendWait(
	ctx context.Context,
	cmd models.Command[T],
) (models.Event[T], error) {
	return i.proc.SendWait(ctx, cmd)
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
	return i.es.Get(ctx, aggregateID)
}

func (i *asynxImpl[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	return i.es.Exists(ctx, aggregateID)
}

func (i *asynxImpl[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	return i.es.Preload(ctx, aggregateID)
}

func (i *asynxImpl[T]) Subscribe(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) (string, error) {
	return i.bus.Subscribe(pattern, handler, opts...)
}

func (i *asynxImpl[T]) Unsubscribe(id string) error {
	return i.bus.Unsubscribe(id)
}

func (i *asynxImpl[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn models.ProjectionHandler[T],
) error {
	return i.es.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
}

// WaitPublish blocks until all async event publishes complete.
// Only for use in tests; do not call in production code.
func (i *asynxImpl[T]) WaitPublish() {
	i.proc.WaitPublish()
	i.bus.WaitForHandlers()
}

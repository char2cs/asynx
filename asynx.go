package asynx

import (
	"context"

	"github.com/char2cs/asynx/models"
)

type Asynx[T any] interface {
	Send(
		ctx context.Context,
		cmd models.Command[T],
	) error
	Shutdown(ctx context.Context) error

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
	Unsubscribe(id string) error

	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn models.ProjectionHandler[T],
	) error
}

type asynxImpl[T any] struct {
	eventStore    models.Store
	snapshotStore models.Store
	bus           models.Bus[T]
	shardingOpts  ShardingOpts
	schemaVersion int
	upcasters     map[int]Upcaster
	panicHandler  models.PanicHandler[T]
}

func (i *asynxImpl[T]) Send(
	ctx context.Context,
	cmd models.Command[T],
) error {
	panic("not implemented")
}

func (i *asynxImpl[T]) Shutdown(ctx context.Context) error {
	panic("not implemented")
}

func (i *asynxImpl[T]) Get(
	ctx context.Context,
	aggregateID string,
) (T, error) {
	panic("not implemented")
}

func (i *asynxImpl[T]) Exists(
	ctx context.Context,
	aggregateID string,
) (bool, error) {
	panic("not implemented")
}

func (i *asynxImpl[T]) Preload(
	ctx context.Context,
	aggregateID string,
) error {
	panic("not implemented")
}

func (i *asynxImpl[T]) Subscribe(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) (string, error) {
	panic("not implemented")
}

func (i *asynxImpl[T]) Unsubscribe(id string) error {
	panic("not implemented")
}

func (i *asynxImpl[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn models.ProjectionHandler[T],
) error {
	panic("not implemented")
}

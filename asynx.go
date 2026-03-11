package asynx

import "context"

type Asynx[T any] interface {
	Send(
		ctx context.Context,
		cmd Command[T],
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
		handler func(Event[T]),
		opts ...SubscriptionOpt[T],
	) (string, error)
	Unsubscribe(id string) error

	Replay(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		toVersion int64,
		fn func(Event[T]),
	) error
}

type asynxImpl[T any] struct {
	eventStore    Store
	snapshotStore Store
	bus           Bus[T]
	shardingOpts  ShardingOpts
	schemaVersion int
	upcasters     map[int]Upcaster
	panicHandler  func(PanicEvent[T])
}

func (i *asynxImpl[T]) Send(
	ctx context.Context,
	cmd Command[T],
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
	handler func(Event[T]),
	opts ...SubscriptionOpt[T],
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
	fn func(Event[T]),
) error {
	panic("not implemented")
}

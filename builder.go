package asynx

import (
	"context"
	"maps"
)

// ShardingOpts configures the processor's worker pool.
// Shards defaults to 8; QueueDepth defaults to 0 (unbounded).
type ShardingOpts struct {
	Shards     int
	QueueDepth int
}

// PanicEvent is delivered to the panic handler when a projection callback panics.
type PanicEvent[T any] struct {
	EventName  string
	Aggregate  T
	Projection func(Event[T])
	Err        error
}

type Upcaster func(
	ctx context.Context,
	eventName string,
	raw []byte,
) ([]byte, error)

type Builder[T any] struct {
	eventStore    Store
	snapshotStore Store
	bus           Bus[T]
	shardingOpts  ShardingOpts
	schemaVersion int
	upcasters     map[int]Upcaster
	panicHandler  func(PanicEvent[T])
}

func New[T any]() *Builder[T] {
	return &Builder[T]{
		schemaVersion: 1,
		upcasters:     make(map[int]Upcaster),
		shardingOpts:  ShardingOpts{Shards: 8},
	}
}

func (b *Builder[T]) WithEventStore(
	s Store,
) *Builder[T] {
	b.eventStore = s
	return b
}

// WithSnapshotStore sets a dedicated snapshot store.
// Defaults to the event store when not provided.
func (b *Builder[T]) WithSnapshotStore(
	s Store,
) *Builder[T] {
	b.snapshotStore = s
	return b
}

func (b *Builder[T]) WithBus(
	bus Bus[T],
) *Builder[T] {
	b.bus = bus
	return b
}

func (b *Builder[T]) WithShardingOpts(
	opts ShardingOpts,
) *Builder[T] {
	b.shardingOpts = opts
	return b
}

func (b *Builder[T]) WithSchemaVersion(
	v int,
) *Builder[T] {
	b.schemaVersion = v
	return b
}

func (b *Builder[T]) WithUpcaster(
	fromVersion int,
	fn Upcaster,
) *Builder[T] {
	b.upcasters[fromVersion] = fn
	return b
}

func (b *Builder[T]) WithPanicHandler(
	fn func(PanicEvent[T]),
) *Builder[T] {
	b.panicHandler = fn
	return b
}

// Build requires WithEventStore; all other options have defaults.
func (b *Builder[T]) Build() (Asynx[T], error) {
	if b.eventStore == nil {
		return nil, ErrMissingEventStore
	}

	snapshotStore := b.snapshotStore
	if snapshotStore == nil {
		snapshotStore = b.eventStore
	}

	return &asynxImpl[T]{
		eventStore:    b.eventStore,
		snapshotStore: snapshotStore,
		bus:           b.bus,
		shardingOpts:  b.shardingOpts,
		schemaVersion: b.schemaVersion,
		upcasters:     maps.Clone(b.upcasters),
		panicHandler:  b.panicHandler,
	}, nil
}

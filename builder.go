package asynx

import (
	"maps"

	"github.com/char2cs/asynx/models"
)

// ShardingOpts configures the processor's worker pool.
// Shards defaults to 8; QueueDepth defaults to 0 (unbounded).
type ShardingOpts struct {
	Shards     int
	QueueDepth int
}

type Builder[T any] struct {
	eventStore    models.Store
	snapshotStore models.Store
	bus           models.Bus[T]
	shardingOpts  ShardingOpts
	schemaVersion int
	upcasters     map[int]models.Upcaster
	panicHandler  models.PanicHandler[T]
}

func New[T any]() *Builder[T] {
	return &Builder[T]{
		schemaVersion: 1,
		upcasters:     make(map[int]models.Upcaster),
		shardingOpts:  ShardingOpts{Shards: 8},
	}
}

func (b *Builder[T]) WithEventStore(
	s models.Store,
) *Builder[T] {
	b.eventStore = s
	return b
}

// WithSnapshotStore sets a dedicated snapshot store.
// Defaults to the event store when not provided.
func (b *Builder[T]) WithSnapshotStore(
	s models.Store,
) *Builder[T] {
	b.snapshotStore = s
	return b
}

func (b *Builder[T]) WithBus(
	bus models.Bus[T],
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
	fn models.Upcaster,
) *Builder[T] {
	b.upcasters[fromVersion] = fn
	return b
}

func (b *Builder[T]) WithPanicHandler(
	fn models.PanicHandler[T],
) *Builder[T] {
	b.panicHandler = fn
	return b
}

// Build requires WithEventStore; all other options have defaults.
func (b *Builder[T]) Build() (Asynx[T], error) {
	if b.eventStore == nil {
		return nil, models.ErrMissingEventStore
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

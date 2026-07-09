package asynx

import (
	"maps"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/models"
)

// ShardingOpts configures the processor's worker pool.
// Shards defaults to 8; QueueDepth defaults to 0 (unbounded).
//
// WorkersPerShard controls how many workers drain each shard's queue and
// defaults to 8 when left zero. It is a correctness knob, not just a
// throughput dial:
//
//   - WorkersPerShard: 1 serializes the load-validate-write cycle for every
//     aggregate on a shard. Commands targeting the same aggregate can never
//     run concurrently, so there is no lost-update or version-conflict window.
//   - WorkersPerShard > 1 (including the default 8) lets a shard execute queued
//     commands in parallel. Two commands that hit the same aggregate at the
//     same time may conflict: one succeeds and the other fails with
//     ErrPipelineFailed for the caller to re-read state and retry.
type ShardingOpts struct {
	Shards          int
	QueueDepth      int
	WorkersPerShard int
}

type Builder[T any] struct {
	eventStore          models.Store
	snapshotStore       models.Store
	bus                 models.Bus[T]
	shardingOpts        ShardingOpts
	schemaVersion       int
	upcasters           map[int]models.Upcaster
	panicHandler        models.PanicHandler[T]
	corruptionHook      func(error)
	publishErrorHandler models.PublishErrorHandler[T]
	forgetHandlers      []models.ForgetHandler[T]
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

// WithCorruptionHook registers a callback invoked when a snapshot cannot be
// deserialized. The hook receives the deserialization error and is called
// before falling back to the cold replay path.
func (b *Builder[T]) WithCorruptionHook(fn func(error)) *Builder[T] {
	b.corruptionHook = fn
	return b
}

// WithPublishErrorHandler sets a callback invoked when Bus.Publish returns a
// non-nil error inside an async publish goroutine. The event has already been
// durably written to the event store; this callback is for observability only.
// When not set, publish errors are silently dropped.
func (b *Builder[T]) WithPublishErrorHandler(fn models.PublishErrorHandler[T]) *Builder[T] {
	b.publishErrorHandler = fn
	return b
}

// WithForgetHandler registers a ForgetHandler at build time.
// Equivalent to calling OnForget after Build. Multiple calls register multiple handlers.
func (b *Builder[T]) WithForgetHandler(fn models.ForgetHandler[T]) *Builder[T] {
	b.forgetHandlers = append(b.forgetHandlers, fn)
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

	activeBus := b.bus
	if activeBus == nil {
		if b.panicHandler != nil {
			activeBus = bus.NewChannelBus[T](
				bus.WithPanicHandler[T](b.panicHandler),
			)
		} else {
			activeBus = bus.NewChannelBus[T]()
		}
	}

	es := eventstore.New[T](
		b.eventStore,
		snapshotStore,
		maps.Clone(b.upcasters),
		b.schemaVersion,
		b.corruptionHook,
	)

	procOpts := []processor.ProcessorOpt[T]{
		processor.WithShards[T](b.shardingOpts.Shards),
		processor.WithQueueDepth[T](b.shardingOpts.QueueDepth),
		processor.WithWorkersPerShard[T](b.shardingOpts.WorkersPerShard),
	}
	if b.publishErrorHandler != nil {
		procOpts = append(procOpts, processor.WithPublishErrorHandler[T](b.publishErrorHandler))
	}
	proc := processor.New(es, activeBus, procOpts...)

	instance := &asynxImpl[T]{
		proc: proc,
		es:   es,
		bus:  activeBus,
	}

	for _, fn := range b.forgetHandlers {
		if _, err := instance.OnForget(fn); err != nil {
			return nil, err
		}
	}

	return instance, nil
}

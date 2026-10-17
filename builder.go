package asynx

import (
	"context"
	"fmt"
	"maps"
	"regexp"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/utils"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/models"
)

// ShardingOpts configures the processor's worker pool.
// Shards defaults to 8; QueueDepth defaults to 0 (unbounded).
//
// WorkersPerShard controls how many workers drain each shard's queue and
// defaults to 8 when left zero. It tunes throughput only: whatever its value,
// commands for one aggregate run strictly one at a time (a per-aggregate lock
// covers load, validate, commit and enqueue), so there is no lost-update or
// version-conflict window and handlers receive an aggregate's events in version
// order. Commands for different aggregates run in parallel up to
// Shards*WorkersPerShard. The lock is in-process: several processes sharing one
// store are protected from corruption by the store's version check, but not
// from conflicts.
type ShardingOpts struct {
	Shards          int
	QueueDepth      int
	WorkersPerShard int
}

type Builder[T any] struct {
	eventStore          models.Store
	snapshotStore       models.SnapshotStore
	bus                 models.Bus[T]
	shardingOpts        ShardingOpts
	schemaVersion       int
	upcasters           map[int]models.Upcaster
	panicHandler        models.PanicHandler[T]
	corruptionHook      func(error)
	publishErrorHandler models.PublishErrorHandler[T]
	forgetHandlers      []models.ForgetHandler[T]
	syncProjections     []syncProjectionDef[T]
	subscribers         []subscriberDef[T]
	snapshotErrHandler  func(context.Context, models.Event[T], error)
}

type syncProjectionDef[T any] struct {
	pattern string
	fn      func(context.Context, models.Tx, models.Event[T]) error
}

type subscriberDef[T any] struct {
	pattern string
	handler models.ProjectionHandler[T]
	opts    []models.SubscriptionOpt[T]
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

// WithSnapshotStore sets the snapshot store. Required — Build returns
// ErrMissingSnapshotStore without it.
//
// This cannot default to the event store: snapshots are upserted cells keyed
// by aggregateID (models.SnapshotStore), not append-only versioned streams
// (models.Store).
func (b *Builder[T]) WithSnapshotStore(
	s models.SnapshotStore,
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

// WithSyncProjection registers a projection that runs inside the transaction
// that appends each matching event. fn writes its own state through
// tx.Native() (for example a *sql.Tx), so the read model commits or rolls back
// together with the event, the command receipt and the delivery entry. If fn
// returns an error the transaction rolls back and Send returns
// ErrPipelineFailed wrapping it: the event never existed.
//
// pattern follows Subscribe: an exact event name, or a regular expression when
// it contains metacharacters. Projections run in registration order.
func (b *Builder[T]) WithSyncProjection(
	pattern string,
	fn func(ctx context.Context, tx models.Tx, evt models.Event[T]) error,
) *Builder[T] {
	b.syncProjections = append(b.syncProjections, syncProjectionDef[T]{pattern: pattern, fn: fn})
	return b
}

// WithSubscriber registers a durable subscriber at build time. Unlike
// Subscribe, whose handlers only see events published while the process is
// alive, a durable subscriber is guaranteed every committed event: each event
// is recorded in the same transaction that commits it and acknowledged once
// every handler has finished, and Build redelivers anything left unacknowledged
// by a crash before it returns, in per-aggregate order.
//
// Delivery is at-least-once. After a crash a handler can see an event twice;
// dedupe on (AggregateID, Version).
func (b *Builder[T]) WithSubscriber(
	pattern string,
	handler models.ProjectionHandler[T],
	opts ...models.SubscriptionOpt[T],
) *Builder[T] {
	b.subscribers = append(b.subscribers, subscriberDef[T]{pattern: pattern, handler: handler, opts: opts})
	return b
}

// WithSnapshotErrorHandler sets a callback invoked when a snapshot write fails.
// Snapshots are a derived cache written after the event commits, so a failure
// never fails the command or blocks delivery. When not set, snapshot errors are
// dropped.
func (b *Builder[T]) WithSnapshotErrorHandler(fn func(context.Context, models.Event[T], error)) *Builder[T] {
	b.snapshotErrHandler = fn
	return b
}

// Build requires WithEventStore and WithSnapshotStore; all other options have
// defaults.
func (b *Builder[T]) Build() (Asynx[T], error) {
	if b.eventStore == nil {
		return nil, models.ErrMissingEventStore
	}

	if b.snapshotStore == nil {
		return nil, models.ErrMissingSnapshotStore
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
		b.snapshotStore,
		maps.Clone(b.upcasters),
		b.schemaVersion,
		b.corruptionHook,
	)

	syncs, err := b.compileSyncProjections()
	if err != nil {
		return nil, err
	}

	procOpts := []processor.ProcessorOpt[T]{
		processor.WithShards[T](b.shardingOpts.Shards),
		processor.WithQueueDepth[T](b.shardingOpts.QueueDepth),
		processor.WithWorkersPerShard[T](b.shardingOpts.WorkersPerShard),
	}
	if len(syncs) > 0 {
		procOpts = append(procOpts, processor.WithSyncProjections(syncs))
	}
	durable, err := b.compileDurableMatch()
	if err != nil {
		return nil, err
	}
	if durable != nil {
		procOpts = append(procOpts, processor.WithDurableDelivery[T](durable))
	}
	if b.snapshotErrHandler != nil {
		procOpts = append(procOpts, processor.WithSnapshotErrorHandler(b.snapshotErrHandler))
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

	// Past this point a failure must not leave the processor's goroutines running.
	built := false
	defer func() {
		if !built {
			_ = proc.Shutdown(context.Background())
		}
	}()

	for _, fn := range b.forgetHandlers {
		if _, err := instance.OnForget(fn); err != nil {
			return nil, err
		}
	}

	for _, sub := range b.subscribers {
		if _, err := activeBus.Subscribe(sub.pattern, sub.handler, sub.opts...); err != nil {
			return nil, err
		}
	}

	if len(b.subscribers) > 0 {
		if err := proc.Recover(context.Background(), recoveryBatch); err != nil {
			return nil, err
		}
	}

	built = true
	return instance, nil
}

// recoveryBatch is how many pending deliveries Build redelivers per round.
const recoveryBatch = 100

func (b *Builder[T]) compileSyncProjections() ([]exec.SyncProjection[T], error) {
	out := make([]exec.SyncProjection[T], 0, len(b.syncProjections))
	for _, def := range b.syncProjections {
		if def.pattern == "" {
			return nil, models.ErrEmptyPattern
		}
		if def.fn == nil {
			return nil, models.ErrNilHandler
		}

		match, err := matcher(def.pattern)
		if err != nil {
			return nil, err
		}
		out = append(out, exec.SyncProjection[T]{Match: match, Fn: def.fn})
	}
	return out, nil
}

// compileDurableMatch returns a predicate that accepts the events at least one
// durable subscriber is interested in, or nil when there are no subscribers.
// Events nobody durable listens to get no delivery entry.
func (b *Builder[T]) compileDurableMatch() (func(string) bool, error) {
	if len(b.subscribers) == 0 {
		return nil, nil
	}

	matchers := make([]func(string) bool, 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		if sub.pattern == "" {
			return nil, models.ErrEmptyPattern
		}
		m, err := matcher(sub.pattern)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}

	return func(name string) bool {
		for _, m := range matchers {
			if m(name) {
				return true
			}
		}
		return false
	}, nil
}

// matcher mirrors the bus's pattern rule: an exact event name, or an
// (unanchored) regular expression when the pattern has metacharacters.
func matcher(pattern string) (func(string) bool, error) {
	if utils.IsExactPattern(pattern) {
		return func(name string) bool { return name == pattern }, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("asynx: invalid pattern %q: %w", pattern, err)
	}
	return re.MatchString, nil
}

package asynx

import (
	"context"
	"github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/pubsub"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/models/subscription"
	"time"
)

// Builder collects the configuration of an Asynx instance and builds it. Get
// one from New, call the With methods that apply, and finish with Build. Every
// With method returns the receiver so the calls chain, and a later call to the
// same option replaces an earlier one, except WithUpcaster, WithForgetHandler,
// WithSyncProjection and WithSubscriber, which accumulate.
//
// WithEventStore and WithSnapshotStore are required; everything else has a
// default. A Builder is not safe for concurrent use and is meant to be used
// for one Build: building twice from one Builder would share its stores and
// bus between the two instances. T is the aggregate type of the instance.
type Builder[T any] struct {
	eventStore          storage.Store
	snapshotStore       storage.SnapshotStore
	bus                 pubsub.Bus[T]
	shardingOpts        ShardingOpts
	schemaVersion       int
	upcasters           map[int]event.Upcaster
	panicHandler        handler.Panic[T]
	corruptionHook      func(error)
	publishErrorHandler handler.PublishError[T]
	forgetHandlers      []handler.Forget[T]
	syncProjections     []syncProjectionDef[T]
	subscribers         []subscriberDef[T]
	snapshotErrHandler  func(context.Context, event.Event[T], error)
	listenerErrHandler  func(error)
	deliveryErrHandler  handler.DeliveryError[T]
	retryInitial        time.Duration
	retryMax            time.Duration
	maxAttempts         int
	deadLetter          handler.DeadLetter[T]
	dispatcherFactory   dispatcher.Factory[T]
}

const defaultShards = 8

// New returns a Builder for aggregates of type T with the defaults: schema
// version 1, no upcasters, 8 shards (see ShardingOpts), the in-memory bus and
// the in-memory dispatcher. It has no event store or snapshot store yet; Build
// fails until both are set.
func New[T any]() *Builder[T] {
	return &Builder[T]{
		schemaVersion: 1,
		upcasters:     make(map[int]event.Upcaster),
		shardingOpts:  ShardingOpts{Shards: defaultShards},
	}
}

// WithEventStore sets the store that holds the event streams, the command
// receipts and the delivery log. It is required: Build returns
// failure.ErrMissingEventStore without it. The store/storetest suite checks an
// implementation of storage.Store against the guarantees the instance relies on.
func (b *Builder[T]) WithEventStore(
	s storage.Store,
) *Builder[T] {
	b.eventStore = s
	return b
}

// WithSnapshotStore sets the store that caches aggregate snapshots. It is
// required: Build returns failure.ErrMissingSnapshotStore without it. Snapshots
// are a derived cache, so losing them costs read speed, never data.
//
// This cannot default to the event store: snapshots are upserted cells keyed
// by aggregateID (storage.SnapshotStore), not append-only versioned streams
// (storage.Store).
func (b *Builder[T]) WithSnapshotStore(
	s storage.SnapshotStore,
) *Builder[T] {
	b.snapshotStore = s
	return b
}

// WithBus replaces the default in-memory bus that delivers committed events to
// the live subscribers (Subscribe, Listen, OnForget). A custom bus must satisfy
// pubsub.Bus. The instance owns the bus it is given: Shutdown closes it, and so
// does a failed Build. WithPanicHandler configures only the default bus, so a
// custom bus handles handler panics its own way.
func (b *Builder[T]) WithBus(
	bus pubsub.Bus[T],
) *Builder[T] {
	b.bus = bus
	return b
}

// WithShardingOpts sets how commands are spread over workers and how many may
// queue; see ShardingOpts for the fields, their defaults and the ordering
// guarantees, which hold whatever the values.
func (b *Builder[T]) WithShardingOpts(
	opts ShardingOpts,
) *Builder[T] {
	b.shardingOpts = opts
	return b
}

// WithSchemaVersion sets the schema version stamped on newly written events
// (default 1). Events written with an older version are migrated to this one
// when they are read, by the upcasters registered with WithUpcaster. Raise it
// whenever the shape of what commands emit changes.
func (b *Builder[T]) WithSchemaVersion(
	v int,
) *Builder[T] {
	b.schemaVersion = v
	return b
}

// WithUpcaster registers the migration for events written at schema version
// fromVersion: fn receives their stored patches and returns patches valid for
// fromVersion+1. Reads chain the upcasters from an event's version up to the
// current one (WithSchemaVersion). Registering a second upcaster for the same
// fromVersion replaces the first. Upcasters must be stateless and safe for
// concurrent use (see event.Upcaster).
func (b *Builder[T]) WithUpcaster(
	fromVersion int,
	fn event.Upcaster,
) *Builder[T] {
	b.upcasters[fromVersion] = fn
	return b
}

// WithPanicHandler sets the handler called when a projection handler or a
// durable handler panics. It receives the context and event of the delivery
// and the recovered value. It applies to the default in-memory bus and to
// durable delivery; a bus supplied with WithBus is responsible for its own
// panics. The panic never reaches the caller of Send.
func (b *Builder[T]) WithPanicHandler(
	fn handler.Panic[T],
) *Builder[T] {
	b.panicHandler = fn
	return b
}

// WithCorruptionHook registers a callback invoked when a snapshot cannot be
// deserialized. The hook receives the deserialization error and is called
// before falling back to the cold replay path, so a corrupt snapshot costs
// speed, not correctness; use the hook to alert on it. It may be called from
// any goroutine that reads an aggregate.
func (b *Builder[T]) WithCorruptionHook(
	fn func(error),
) *Builder[T] {
	b.corruptionHook = fn
	return b
}

// WithPublishErrorHandler sets a callback invoked when Bus.Publish returns a
// non-nil error inside an async publish goroutine. The event has already been
// durably written to the event store; this callback is for observability only.
// When not set, publish errors are silently dropped.
func (b *Builder[T]) WithPublishErrorHandler(
	fn handler.PublishError[T],
) *Builder[T] {
	b.publishErrorHandler = fn
	return b
}

// WithForgetHandler registers a handler.Forget at build time, so no
// Forget can run before it is in place. It is equivalent to calling OnForget on
// the built instance, except that a registration failure fails Build. Multiple
// calls register multiple handlers, in the order of the calls.
func (b *Builder[T]) WithForgetHandler(
	fn handler.Forget[T],
) *Builder[T] {
	b.forgetHandlers = append(
		b.forgetHandlers,
		fn,
	)
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
	fn func(ctx context.Context, tx storage.Tx, evt event.Event[T]) error,
) *Builder[T] {
	b.syncProjections = append(
		b.syncProjections,
		syncProjectionDef[T]{pattern: pattern, fn: fn},
	)
	return b
}

// WithSubscriber registers a durable subscriber at build time. Unlike
// Subscribe, whose handlers only see events published while the process is
// alive and cannot report failure, a durable subscriber is guaranteed every
// committed event it matches:
//
//   - Each event is recorded in the same transaction that commits it, and
//     acknowledged only after every matching durable handler returned nil.
//     Build queues anything left unacknowledged by a crash for redelivery,
//     in per-aggregate order, and returns without waiting for the handlers.
//   - A handler that returns an error, panics, or fails after exceeding its
//     timeout leaves the delivery pending and is retried with backoff
//     (WithDeliveryRetry) up to WithDeliveryMaxAttempts times. The aggregate's
//     later events wait behind it, for every durable subscriber, so version 6
//     is never delivered before version 5 succeeded. Other aggregates are
//     unaffected. After the last attempt the event is dead-lettered
//     (WithDeadLetterHandler) or the aggregate is parked until
//     ResumeDeliveries. Observe failures with WithDeliveryErrorHandler.
//   - Every durable subscriber also receives the deletion marker Forget writes
//     (event.ForgetEventName), whatever its pattern, so state keyed by
//     (AggregateID, Version) can be cleared.
//   - A timeout (WithHandlerTimeout) is an alarm: it is reported but the handler
//     is still waited for. WithFallback is not supported and makes Build fail
//     with ErrFallbackUnsupported.
//
// Delivery is at-least-once: after a crash, or when a sibling handler of the
// same event failed, a handler can see an event again. Make it idempotent
// (dedupe on AggregateID and Version). SendWait returns once every handler has
// been attempted once; it does not wait for retries.
//
// pattern follows Subscribe: an exact event name, or a regular expression when
// it contains metacharacters.
func (b *Builder[T]) WithSubscriber(
	pattern string,
	callback handler.Durable[T],
	opts ...subscription.Opt[T],
) *Builder[T] {
	b.subscribers = append(
		b.subscribers,
		subscriberDef[T]{pattern: pattern, handler: callback, opts: opts},
	)
	return b
}

// WithListenerErrorHandler sets a callback invoked when removing the
// subscription behind a Listen or SubscribeWait fails: the bus refused the
// Unsubscribe that the listener's unsubscribe function, or the automatic
// unsubscribe after the last of count events, performed. The error wraps the
// bus's. Those paths have no error to return (the unsubscribe function is a
// func()), so the callback is the only way to learn of it; SubscribeWait
// additionally returns a failure of its own unsubscribe, joined with its
// result. When not set, such failures are dropped. The default bus never fails
// to unsubscribe, so this matters for a bus supplied with WithBus. It may be
// called from any goroutine.
func (b *Builder[T]) WithListenerErrorHandler(
	fn func(error),
) *Builder[T] {
	b.listenerErrHandler = fn
	return b
}

// WithDeliveryErrorHandler sets a callback invoked for every failed attempt of
// a durable delivery (handler error, panic or timeout), with the attempt number
// of the current failing streak. When not set, failures are retried silently.
func (b *Builder[T]) WithDeliveryErrorHandler(
	fn handler.DeliveryError[T],
) *Builder[T] {
	b.deliveryErrHandler = fn
	return b
}

// WithDeliveryRetry sets the retry backoff of failed durable deliveries: the
// first retry waits about initial, then the delay doubles up to about max.
// Defaults: 100ms and 30s. Each wait is randomised within its upper half
// (jitter), so deliveries that failed together, because a downstream service
// went down, do not retry in lockstep.
func (b *Builder[T]) WithDeliveryRetry(
	initial time.Duration,
	max time.Duration,
) *Builder[T] {
	b.retryInitial, b.retryMax = initial, max
	return b
}

// WithDeliveryMaxAttempts caps how often a failing durable delivery is
// attempted (default 20, about six minutes with the default backoff). When the
// cap is reached the delivery goes to the dead-letter handler
// (WithDeadLetterHandler) if one accepts it; otherwise the aggregate is parked:
// its failed event and every later one stay pending in the store, nothing more
// is delivered for it, and the failure is reported to the
// delivery error handler with ErrDeliveryParked. Other aggregates are
// unaffected. ResumeDeliveries (or a restart) redelivers a parked aggregate.
func (b *Builder[T]) WithDeliveryMaxAttempts(
	n int,
) *Builder[T] {
	b.maxAttempts = n
	return b
}

// WithDeadLetterHandler sets the handler called when a durable delivery ran out
// of attempts. Its error argument holds the failure of every subscriber that
// failed on the last attempt, each a *failure.SubscriberError naming the
// subscriber by pattern and registration order (such as "OrderUpdated#2").
// Returning nil acknowledges the event as dealt with (typically
// after storing it somewhere for inspection) and lets the aggregate's later
// events flow; returning an error parks the aggregate.
func (b *Builder[T]) WithDeadLetterHandler(
	fn handler.DeadLetter[T],
) *Builder[T] {
	b.deadLetter = fn
	return b
}

// WithSnapshotErrorHandler sets a callback that observes failed snapshot
// writes. Snapshots are a derived cache, so a failure never fails the command
// or the read and never blocks delivery. Two kinds of write are observed:
//
//   - The snapshot a command asks for (ShouldSnapshot), written after its event
//     committed. fn receives the committed event in full and runs on the
//     goroutine that executed the command.
//   - The auto-snapshot a read (Get, Load or Preload) caches after it had to
//     upcast old events. fn receives an Event with only what a read knows:
//     AggregateID, Version (the version the snapshot was meant for), ID (the
//     id of that version's event), SchemaVersion (the current schema version)
//     and Aggregate (the state that was not cached); EventName, OccurredAt and
//     PreviousAggregate are zero. fn runs synchronously on the goroutine that
//     performed the read, before the read returns; for a lock-free Get it runs
//     while the aggregate's command lock is held, so it should return quickly.
//
// A failed read-side write does not change what the read returns. A panic in
// fn during a read is contained and never breaks the read. When not set, or
// set to nil, snapshot errors of both kinds are dropped.
func (b *Builder[T]) WithSnapshotErrorHandler(
	fn func(context.Context, event.Event[T], error),
) *Builder[T] {
	b.snapshotErrHandler = fn
	return b
}

// WithDispatcher plugs in the dispatcher that delivers committed events to the
// live and durable subscribers, in place of the in-memory default. A dispatcher
// is the component that takes each committed event and runs the subscribers'
// handlers in order per aggregate, retrying and parking an aggregate when the
// attempts run out. The library ships only the in-memory implementation; the
// contract a replacement must keep is dispatcher.Dispatcher, and the in-memory
// implementation is checked against it by the library's own tests. The factory
// is called once per Build, with the configuration the instance was built with:
// the bus, the durable subscribers, the handlers, and the retry and attempt
// settings with their defaults already applied (20 attempts, a backoff of 100ms
// doubling up to 30s). It returns the dispatcher the instance owns, which
// Shutdown interrupts and then closes. A factory error fails Build, and what
// was built so far is shut down. A nil factory is ignored.
//
// The dispatcher must keep the guarantees documented on dispatcher.Dispatcher. The
// delivery log, the aggregate lock and recovery stay with asynx: Build reads
// the log and hands the pending deliveries to the dispatcher as
// dispatch.KindRedelivery requests.
func (b *Builder[T]) WithDispatcher(
	factory dispatcher.Factory[T],
) *Builder[T] {
	b.dispatcherFactory = factory
	return b
}

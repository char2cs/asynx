package asynx

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/utils"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/internal/processor/exec"
	internalutils "github.com/char2cs/asynx/internal/utils"
	"github.com/char2cs/asynx/models"
)

// ShardingOpts configures the processor's worker pool.
// Shards defaults to 8.
//
// QueueDepth bounds the commands that may wait behind the one running for an
// aggregate: further commands to it are refused with ErrQueueFull, and other
// aggregates keep their workers. It is also the size of the shard's intake
// buffer, which all of the shard's aggregates share: while a flood fills it, a
// command to another aggregate on the same shard can be refused with
// ErrQueueFull too, so size it for the bursts you expect. It defaults to WorkersPerShard when left zero, and is never less than
// it. The queue is bounded: it is not unlimited when zero.
//
// WorkersPerShard controls how many workers drain each shard's queue and
// defaults to 8 when left zero. It tunes throughput only: whatever its value,
// commands for one aggregate run strictly one at a time (a per-aggregate lock
// covers load, validate, commit and enqueue), so there is no lost-update or
// version-conflict window and handlers receive an aggregate's events in version
// order. Commands for different aggregates run in parallel up to
// Shards*WorkersPerShard: a command waiting for a busy aggregate does not hold
// a worker, so one slow aggregate occupies at most one of its shard's workers.
// Work inside a store transaction (WithSyncProjection) is not covered: a store
// that runs one transaction at a time serialises every command behind it.
// The lock is in-process: several processes sharing one
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
	deliveryErrHandler  models.DeliveryErrorHandler[T]
	retryInitial        time.Duration
	retryMax            time.Duration
	maxAttempts         int
	deadLetter          models.DeadLetterHandler[T]
	dispatcherFactory   models.DispatcherFactory[T]
}

type syncProjectionDef[T any] struct {
	pattern string
	fn      func(context.Context, models.Tx, models.Event[T]) error
}

type subscriberDef[T any] struct {
	pattern string
	handler models.DurableHandler[T]
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
//     (models.ForgetEventName), whatever its pattern, so state keyed by
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
	handler models.DurableHandler[T],
	opts ...models.SubscriptionOpt[T],
) *Builder[T] {
	b.subscribers = append(b.subscribers, subscriberDef[T]{pattern: pattern, handler: handler, opts: opts})
	return b
}

// WithDeliveryErrorHandler sets a callback invoked for every failed attempt of
// a durable delivery (handler error, panic or timeout), with the attempt number
// of the current failing streak. When not set, failures are retried silently.
func (b *Builder[T]) WithDeliveryErrorHandler(fn models.DeliveryErrorHandler[T]) *Builder[T] {
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
func (b *Builder[T]) WithDeliveryMaxAttempts(n int) *Builder[T] {
	b.maxAttempts = n
	return b
}

// WithDeadLetterHandler sets the handler called when a durable delivery ran out
// of attempts. Its error argument holds the failure of every subscriber that
// failed on the last attempt, each a *models.SubscriberError naming the
// subscriber by pattern and registration order (such as "OrderUpdated#2").
// Returning nil acknowledges the event as dealt with (typically
// after storing it somewhere for inspection) and lets the aggregate's later
// events flow; returning an error parks the aggregate.
func (b *Builder[T]) WithDeadLetterHandler(fn models.DeadLetterHandler[T]) *Builder[T] {
	b.deadLetter = fn
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

// WithDispatcher plugs in the dispatcher that delivers committed events to the
// live and durable subscribers, in place of the in-memory default. A dispatcher
// is the component that takes each committed event and runs the subscribers'
// handlers in order per aggregate, retrying and parking an aggregate when the
// attempts run out. The library ships only the in-memory implementation; the
// contract a replacement must keep is models.Dispatcher, and the
// dispatchertest package (dispatchertest.Run) verifies an implementation
// against it. The factory
// is called once per Build, with the configuration the instance was built with:
// the bus, the durable subscribers, the handlers, and the retry and attempt
// settings with their defaults already applied (20 attempts, a backoff of 100ms
// doubling up to 30s). It returns the dispatcher the instance owns, which
// Shutdown interrupts and then closes. A factory error fails Build, and what
// was built so far is shut down. A nil factory is ignored.
//
// The dispatcher must keep the guarantees documented on models.Dispatcher. The
// delivery log, the aggregate lock and recovery stay with asynx: Build reads
// the log and hands the pending deliveries to the dispatcher as
// models.DispatchRedelivery requests.
func (b *Builder[T]) WithDispatcher(
	factory models.DispatcherFactory[T],
) *Builder[T] {
	b.dispatcherFactory = factory
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

	activeBus := b.activeBus()

	es := eventstore.New[T](
		b.eventStore,
		b.snapshotStore,
		maps.Clone(b.upcasters),
		b.schemaVersion,
		b.corruptionHook,
	)

	procOpts, err := b.processorOpts()
	if err != nil {
		return nil, err
	}
	proc, err := processor.New(es, activeBus, procOpts...)
	if err != nil {
		closeAbandonedBus(activeBus)
		return nil, err
	}

	instance := &asynxImpl[T]{
		proc: proc,
		es:   es,
		bus:  activeBus,
	}

	built := false
	defer shutdownUnlessBuilt(proc, &built)

	err = b.finish(instance, proc)
	if err != nil {
		return nil, err
	}

	built = true
	return instance, nil
}

func (b *Builder[T]) activeBus() models.Bus[T] {
	if b.bus != nil {
		return b.bus
	}
	if b.panicHandler != nil {
		return bus.NewChannelBus[T](
			bus.WithPanicHandler[T](b.panicHandler),
		)
	}
	return bus.NewChannelBus[T]()
}

func (b *Builder[T]) processorOpts() ([]processor.ProcessorOpt[T], error) {
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
	durableSubs, durableMatch, err := b.compileDurable()
	if err != nil {
		return nil, err
	}
	procOpts = append(procOpts, b.durableOpts(durableSubs, durableMatch)...)
	if b.snapshotErrHandler != nil {
		procOpts = append(procOpts, processor.WithSnapshotErrorHandler(b.snapshotErrHandler))
	}
	if b.publishErrorHandler != nil {
		procOpts = append(procOpts, processor.WithPublishErrorHandler[T](b.publishErrorHandler))
	}
	if b.dispatcherFactory != nil {
		procOpts = append(procOpts, processor.WithDispatcherFactory(b.dispatcherFactory))
	}
	return procOpts, nil
}

func (b *Builder[T]) durableOpts(
	durableSubs []models.DurableSub[T],
	durableMatch func(string) bool,
) []processor.ProcessorOpt[T] {
	if durableMatch == nil {
		return nil
	}
	opts := []processor.ProcessorOpt[T]{
		processor.WithDurableDelivery[T](durableMatch),
		processor.WithDurableSubscribers(durableSubs),
	}
	if b.deliveryErrHandler != nil {
		opts = append(opts, processor.WithDeliveryErrorHandler(b.deliveryErrHandler))
	}
	if b.panicHandler != nil {
		opts = append(opts, processor.WithDeliveryPanicHandler(b.panicHandler))
	}
	opts = append(opts, processor.WithDeliveryRetry[T](b.retryInitial, b.retryMax))
	if b.maxAttempts > 0 {
		opts = append(opts, processor.WithDeliveryMaxAttempts[T](b.maxAttempts))
	}
	if b.deadLetter != nil {
		opts = append(opts, processor.WithDeadLetterHandler(b.deadLetter))
	}
	return opts
}

func (b *Builder[T]) finish(
	instance *asynxImpl[T],
	proc *processor.Processor[T],
) error {
	err := internalutils.FirstError(
		b.forgetHandlers,
		func(fn models.ForgetHandler[T]) error {
			_, err := instance.OnForget(fn)
			return err
		},
	)
	if err != nil {
		return err
	}

	if len(b.subscribers) == 0 {
		return nil
	}
	return proc.Recover(context.Background(), recoveryBatch)
}

func shutdownUnlessBuilt[T any](
	proc *processor.Processor[T],
	built *bool,
) {
	if *built {
		return
	}
	sctx, cancel := context.WithTimeout(context.Background(), failedBuildShutdown)
	defer cancel()
	_ = proc.Shutdown(sctx)
}

// closeAbandonedBus closes the bus of a Build that failed before a processor
// existed to shut it down, within the same bound.
func closeAbandonedBus[T any](
	activeBus models.Bus[T],
) {
	ctx, cancel := context.WithTimeout(context.Background(), failedBuildShutdown)
	defer cancel()
	_ = activeBus.Close(ctx)
}

// failedBuildShutdown bounds how long a failed Build waits for the instance it
// abandons to shut down, so a hung recovered handler cannot hang Build too.
const failedBuildShutdown = 30 * time.Second

// recoveryBatch is how many pending deliveries Build redelivers per round.
const recoveryBatch = 100

func (b *Builder[T]) compileSyncProjections() ([]exec.SyncProjection[T], error) {
	out := make([]exec.SyncProjection[T], 0, len(b.syncProjections))
	var failure error
	for _, def := range b.syncProjections {
		sp, err := compileSyncProjection(def)
		failure = cmp.Or(failure, err)
		out = append(out, sp)
	}
	if failure != nil {
		return nil, failure
	}
	return out, nil
}

func compileSyncProjection[T any](
	def syncProjectionDef[T],
) (exec.SyncProjection[T], error) {
	if def.pattern == "" {
		return exec.SyncProjection[T]{}, models.ErrEmptyPattern
	}
	if def.fn == nil {
		return exec.SyncProjection[T]{}, models.ErrNilHandler
	}

	match, err := matcher(def.pattern)
	if err != nil {
		return exec.SyncProjection[T]{}, err
	}
	return exec.SyncProjection[T]{Match: match, Fn: def.fn}, nil
}

// compileDurable validates the durable subscribers and returns them with a
// predicate accepting the events at least one of them wants (nil when there are
// none). Events nobody durable listens to get no delivery entry.
func (b *Builder[T]) compileDurable() ([]models.DurableSub[T], func(string) bool, error) {
	if len(b.subscribers) == 0 {
		return nil, nil, nil
	}

	subs, err := b.compileSubscribers()
	if err != nil {
		return nil, nil, err
	}

	return subs, wantedBy(subs), nil
}

func (b *Builder[T]) compileSubscribers() ([]models.DurableSub[T], error) {
	subs := make([]models.DurableSub[T], 0, len(b.subscribers))
	var failure error
	for i, def := range b.subscribers {
		sub, err := compileSubscriber(i, def)
		failure = cmp.Or(failure, err)
		subs = append(subs, sub)
	}
	if failure != nil {
		return nil, failure
	}
	return subs, nil
}

func compileSubscriber[T any](
	index int,
	def subscriberDef[T],
) (models.DurableSub[T], error) {
	if def.pattern == "" {
		return models.DurableSub[T]{}, models.ErrEmptyPattern
	}
	if def.handler == nil {
		return models.DurableSub[T]{}, models.ErrNilHandler
	}

	cfg := &models.SubscriptionConfig[T]{}
	for _, opt := range def.opts {
		opt(cfg)
	}
	if cfg.Fallback != nil {
		return models.DurableSub[T]{}, models.ErrFallbackUnsupported
	}

	m, err := matcher(def.pattern)
	if err != nil {
		return models.DurableSub[T]{}, err
	}
	name := fmt.Sprintf("%s#%d", def.pattern, index+1)
	return models.DurableSub[T]{Name: name, Match: m, Handler: def.handler, Timeout: cfg.Timeout}, nil
}

func wantedBy[T any](subs []models.DurableSub[T]) func(string) bool {
	return func(name string) bool {
		return slices.ContainsFunc(subs, func(sub models.DurableSub[T]) bool {
			return sub.Match(name)
		})
	}
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

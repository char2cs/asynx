// Package processor coordinates command routing and execution lifecycle for Asynx.
//
// Processor[T] routes incoming commands to shards via consistent hashing,
// manages graceful shutdown, and exposes Send/SendWait/Shutdown interfaces.
//   - router     — FNV-1a hash-based consistent shard selection
//   - pool       — Shard-based worker pool for concurrent command execution
//   - executor   — Passed to pool; executes Load->Validate->Write->Dispatch pipeline
//
// All command execution is non-blocking via channels. Send and SendWait block until either
// the command completes, context cancels, or the queue is full. Send dispatches events
// asynchronously; SendWait dispatches synchronously. Shutdown drains in-flight work,
// closes the dispatcher, then closes the bus.
package processor

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/commands"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/processor/queue"
	asynxmd "github.com/char2cs/asynx/models"
)

type Processor[T any] struct {
	es            *eventstore.EventStore[T]
	pool          *pool.ShardPool[T]
	router        *queue.Router
	executor      *exec.CommandExecutor[T]
	dispatcher    *dispatcher.Dispatcher[T]
	bus           asynxmd.Bus[T]
	shuttingDown  atomic.Bool
	onSendPending func()
}

type ProcessorOpt[T any] func(*processorConfig[T])

type processorConfig[T any] struct {
	shards              int
	queueDepth          int
	workersPerShard     int
	publishErrorHandler asynxmd.PublishErrorHandler[T]
	executorOpts        []exec.Opt[T]
	dispatcherOpts      []dispatcher.Opt[T]
}

// WithDurableSubscribers registers the durable subscribers the dispatcher runs,
// with retry, for durable deliveries.
func WithDurableSubscribers[T any](subs []dispatcher.DurableSub[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithDurableSubs(subs))
	}
}

// WithDeliveryErrorHandler observes failed durable delivery attempts.
func WithDeliveryErrorHandler[T any](fn asynxmd.DeliveryErrorHandler[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithDeliveryErrorHandler(fn))
	}
}

// WithDeliveryPanicHandler is called when a durable handler panics.
func WithDeliveryPanicHandler[T any](fn asynxmd.PanicHandler[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithDeliveryPanicHandler(fn))
	}
}

// WithDeliveryRetry sets the retry backoff of failed durable deliveries.
func WithDeliveryRetry[T any](initial, max time.Duration) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithRetryBackoff[T](initial, max))
	}
}

// WithSyncProjections registers projections that run inside each event's
// transaction.
func WithSyncProjections[T any](p []exec.SyncProjection[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.executorOpts = append(cfg.executorOpts, exec.WithSyncProjections(p))
	}
}

// WithDurableDelivery records a delivery entry for every event whose name
// match accepts and acknowledges it once handlers finish; see Processor.Recover.
func WithDurableDelivery[T any](match func(eventName string) bool) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.executorOpts = append(cfg.executorOpts, exec.WithDurableDelivery[T](match))
	}
}

// WithSnapshotErrorHandler sets the callback for failed post-commit snapshots.
func WithSnapshotErrorHandler[T any](fn func(context.Context, asynxmd.Event[T], error)) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.executorOpts = append(cfg.executorOpts, exec.WithSnapshotErrorHandler(fn))
	}
}

func WithShards[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if count > 0 {
			cfg.shards = count
		}
	}
}

func WithQueueDepth[T any](depth int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if depth > 0 {
			cfg.queueDepth = depth
		}
	}
}

func WithWorkersPerShard[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if count > 0 {
			cfg.workersPerShard = count
		}
	}
}

// WithPublishErrorHandler sets a callback invoked when Bus.PublishSync returns a
// non-nil error inside the dispatcher. When not set, publish errors are silently
// dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.publishErrorHandler = fn
	}
}

func New[T any](
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...ProcessorOpt[T],
) *Processor[T] {
	cfg := &processorConfig[T]{
		shards:          8,
		queueDepth:      -1,
		workersPerShard: 8,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.queueDepth <= 0 {
		cfg.queueDepth = cfg.workersPerShard
	}

	dispatcherOpts := cfg.dispatcherOpts
	if cfg.publishErrorHandler != nil {
		dispatcherOpts = append(dispatcherOpts, dispatcher.WithPublishErrorHandler[T](cfg.publishErrorHandler))
	}

	var d *dispatcher.Dispatcher[T]
	if bus != nil {
		d = dispatcher.New(bus, dispatcherOpts...)
	}

	executor := exec.New(es, d, cfg.executorOpts...)
	return &Processor[T]{
		es: es,
		pool: pool.New(
			executor,
			cfg.shards,
			cfg.queueDepth,
			cfg.workersPerShard,
		),
		router: queue.New(
			cfg.shards,
		),
		executor:   executor,
		dispatcher: d,
		bus:        bus,
	}
}

func (p *Processor[T]) Send(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if p.isShuttingDown() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}

	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	shard := p.pool.Shards()[shardIndex]
	envelope := &models.CommandEnvelope[T]{
		Cmd:        cmd,
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[T], 1),
	}

	return p.sendAndWait(ctx, shard, envelope)
}

func (p *Processor[T]) SendWait(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if p.isShuttingDown() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}

	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	shard := p.pool.Shards()[shardIndex]
	envelope := &models.CommandEnvelope[T]{
		Cmd:          cmd,
		Ctx:          ctx,
		ResultChan:   make(chan models.CommandResult[T], 1),
		WaitHandlers: true,
	}

	return p.sendAndWait(ctx, shard, envelope)
}

func (p *Processor[T]) isShuttingDown() bool {
	return p.shuttingDown.Load()
}

func (p *Processor[T]) sendAndWait(
	ctx context.Context,
	shard *pool.Shard[T],
	envelope *models.CommandEnvelope[T],
) (asynxmd.Event[T], error) {
	select {
	case shard.CommandChan() <- envelope:
	case <-ctx.Done():
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	default:
		return asynxmd.Event[T]{}, asynxmd.ErrQueueFull
	}

	if p.onSendPending != nil {
		p.onSendPending()
	}

	select {
	case result := <-envelope.ResultChan:
		return result.Event, result.Err
	case <-ctx.Done():
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}
}

func (p *Processor[T]) Shutdown(ctx context.Context) error {
	if !p.setShuttingDown() {
		return asynxmd.ErrAlreadyShuttingDown
	}

	if err := p.pool.Drain(ctx); err != nil {
		return err
	}

	if err := p.closeDispatcher(ctx); err != nil {
		return err
	}

	return p.closeBus(ctx)
}

func (p *Processor[T]) setShuttingDown() bool {
	return p.shuttingDown.CompareAndSwap(false, true)
}

func (p *Processor[T]) closeDispatcher(ctx context.Context) error {
	if p.dispatcher == nil {
		return nil
	}
	return p.dispatcher.Close(ctx)
}

func (p *Processor[T]) closeBus(ctx context.Context) error {
	if p.bus == nil {
		return nil
	}
	return p.bus.Close(ctx)
}

// ForTesting: WaitPublish blocks until all dispatched events have been delivered.
// Do not call in production code.
func (p *Processor[T]) WaitPublish() {
	if p.dispatcher != nil {
		p.dispatcher.WaitIdle()
	}
}

// ForTesting: SetOnSendPending sets a callback invoked after a command is
// enqueued but before Send or SendWait blocks waiting for its result.
// Do not call in production code.
func (p *Processor[T]) SetOnSendPending(fn func()) {
	p.onSendPending = fn
}

// Forget erases the aggregate on its shard worker, fenced by the aggregate's
// lock so no command can interleave. It returns once the forget handlers have
// run and the aggregate is erased.
func (p *Processor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	if p.isShuttingDown() {
		return asynxmd.ErrShuttingDown
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.ErrContextCancelled
	}

	cmd := commands.NewForgetCommand[T](aggregateID)
	shard := p.pool.Shards()[p.router.Route(aggregateID)]
	envelope := &models.CommandEnvelope[T]{
		Cmd:          cmd,
		Ctx:          ctx,
		ResultChan:   make(chan models.CommandResult[T], 1),
		WaitHandlers: true,
		Forget:       true,
	}

	_, err := p.sendAndWait(ctx, shard, envelope)
	return err
}

// Recover re-enqueues every unacknowledged delivery, in enqueue order, on the
// per-aggregate dispatcher. It returns once they are all queued, not once they
// are handled: a handler that keeps failing must not stop the process from
// starting. Ordering against live commands is still guaranteed, because a live
// event enqueues behind the recovered events of its aggregate in the same FIFO.
// Handlers run, retry on failure and acknowledge exactly as for live events.
//
// Deliveries whose event no longer exists are acknowledged and skipped.
// Delivery is at-least-once: an event whose handlers finished but whose ack was
// lost to a crash is delivered again.
func (p *Processor[T]) Recover(ctx context.Context, batch int) error {
	if p.dispatcher == nil {
		return nil
	}
	if batch <= 0 {
		batch = 100
	}

	store := p.es.Store()
	queued := make(map[asynxmd.Delivery]struct{})

	for {
		// Entries already queued sit at the front (the log is in enqueue
		// order), so asking for that many plus a batch always reaches new ones.
		pending, err := store.PendingDeliveries(ctx, len(queued)+batch)
		if err != nil {
			return fmt.Errorf("asynx: recovery: %w", err)
		}

		fresh := 0
		for _, d := range pending {
			if _, again := queued[d]; again {
				continue
			}
			queued[d] = struct{}{}
			fresh++

			if err := p.redeliver(ctx, d); err != nil {
				return fmt.Errorf("asynx: recovery: %w", err)
			}
		}
		if fresh == 0 {
			return nil
		}
	}
}

// redeliver rebuilds the event behind d and enqueues it. When the event no
// longer exists the delivery is acknowledged instead.
func (p *Processor[T]) redeliver(ctx context.Context, d asynxmd.Delivery) error {
	store := p.es.Store()

	var event asynxmd.Event[T]
	found := false
	err := p.es.Replay(ctx, d.AggregateID, d.Version, d.Version,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if evt.Version == d.Version {
				event, found = evt, true
			}
		})
	if err != nil {
		return err
	}

	if !found {
		return store.AckDelivery(ctx, d.AggregateID, d.Version)
	}

	_, err = p.dispatcher.Enqueue(ctx, event, func(ctx context.Context) error {
		return store.AckDelivery(ctx, d.AggregateID, d.Version)
	})
	return err
}

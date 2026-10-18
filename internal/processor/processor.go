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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
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
	lifeMu        sync.Mutex     // orders Shutdown against Send and Forget calls starting
	shutdown      shutdownState  // guarded by lifeMu
	sends         sync.WaitGroup // Send and SendWait calls in flight
	forgets       sync.WaitGroup // Forget calls in flight
	onSendPending func()
}

// shutdownState lets a Shutdown that failed (its context ended) be retried.
type shutdownState int

const (
	notShutDown shutdownState = iota
	shuttingDownNow
	shutdownFailed
	shutDown
)

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

// WithDeliveryMaxAttempts caps the attempts of a failing durable delivery.
func WithDeliveryMaxAttempts[T any](n int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithMaxAttempts[T](n))
	}
}

// WithDeadLetterHandler sets the handler for deliveries that ran out of attempts.
func WithDeadLetterHandler[T any](fn asynxmd.DeadLetterHandler[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.dispatcherOpts = append(cfg.dispatcherOpts, dispatcher.WithDeadLetterHandler(fn))
	}
}

// WithDeliveryRetry sets the retry backoff of failed durable deliveries.
func WithDeliveryRetry[T any](
	initial time.Duration,
	max time.Duration,
) ProcessorOpt[T] {
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
	es.GuardAutoSnapshots(executor.WithLockIfFree)
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
	if !p.beginSend() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}
	defer p.sends.Done()

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
	if !p.beginSend() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}
	defer p.sends.Done()

	if p.dispatcher != nil && p.dispatcher.InHandler(ctx, cmd.AggregateID()) {
		return asynxmd.Event[T]{}, asynxmd.ErrSendWaitFromHandler
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

// beginSend registers a Send or SendWait in flight, unless shutdown has begun.
// Shutdown waits for the registered ones, so none is stranded in a queue the
// pool has stopped reading.
func (p *Processor[T]) beginSend() bool {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.isShuttingDown() {
		return false
	}
	p.sends.Add(1)
	return true
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
		if result.Err != nil || result.Wait == nil {
			return result.Event, result.Err
		}
		if err := result.Wait(ctx); err != nil {
			if errors.Is(err, asynxmd.ErrContextCancelled) {
				return result.Event, asynxmd.ErrContextCancelled
			}
			return result.Event, err
		}
		return result.Event, nil
	case <-ctx.Done():
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}
}

// Shutdown stops accepting work, waits for the commands and Forgets in flight,
// then closes the dispatcher and the bus. If ctx ends first it returns the
// error and can be called again to finish; once it has completed, or while
// another call runs, it returns ErrAlreadyShuttingDown.
func (p *Processor[T]) Shutdown(ctx context.Context) error {
	if !p.beginShutdown() {
		return asynxmd.ErrAlreadyShuttingDown
	}
	err := p.runShutdown(ctx)
	p.endShutdown(err)
	return err
}

func (p *Processor[T]) runShutdown(ctx context.Context) error {
	if p.dispatcher != nil {
		p.dispatcher.Interrupt()
	}

	if err := waitGroup(ctx, &p.sends); err != nil {
		return err
	}

	if err := p.pool.Drain(ctx); err != nil {
		return err
	}

	if err := waitGroup(ctx, &p.forgets); err != nil {
		return err
	}

	if err := p.closeDispatcher(ctx); err != nil {
		return err
	}

	return p.closeBus(ctx)
}

func (p *Processor[T]) beginShutdown() bool {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if p.shutdown == shuttingDownNow || p.shutdown == shutDown {
		return false
	}
	p.shutdown = shuttingDownNow
	p.shuttingDown.Store(true)
	return true
}

func (p *Processor[T]) endShutdown(err error) {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if err != nil {
		p.shutdown = shutdownFailed
		return
	}
	p.shutdown = shutDown
}

// waitGroup waits for wg, or for ctx.
func waitGroup(
	ctx context.Context,
	wg *sync.WaitGroup,
) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

// Forget erases the aggregate; see CommandExecutor.Forget. It does not occupy a
// shard worker while waiting for handlers, so a slow handler cannot stall the
// shard's other aggregates.
func (p *Processor[T]) Forget(
	ctx context.Context,
	aggregateID string,
) error {
	p.lifeMu.Lock()
	if p.isShuttingDown() {
		p.lifeMu.Unlock()
		return asynxmd.ErrShuttingDown
	}
	p.forgets.Add(1)
	p.lifeMu.Unlock()
	defer p.forgets.Done()

	if err := ctx.Err(); err != nil {
		return asynxmd.ErrContextCancelled
	}
	return p.executor.Forget(ctx, aggregateID)
}

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
	"sync/atomic"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/processor/queue"
	asynxmd "github.com/char2cs/asynx/models"
)

type Processor[T any] struct {
	pool         *pool.ShardPool[T]
	router       *queue.Router
	executor     *exec.CommandExecutor[T]
	dispatcher   *dispatcher.Dispatcher[T]
	bus          asynxmd.Bus[T]
	shuttingDown atomic.Bool
	onSendPending func()
}

type ProcessorOpt[T any] func(*processorConfig[T])

type processorConfig[T any] struct {
	shards              int
	queueDepth          int
	workersPerShard     int
	publishErrorHandler asynxmd.PublishErrorHandler[T]
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

	var dispatcherOpts []dispatcher.Opt[T]
	if cfg.publishErrorHandler != nil {
		dispatcherOpts = append(dispatcherOpts, dispatcher.WithPublishErrorHandler[T](cfg.publishErrorHandler))
	}

	var d *dispatcher.Dispatcher[T]
	if bus != nil {
		d = dispatcher.New(bus, dispatcherOpts...)
	}

	executor := exec.New(es, d)
	return &Processor[T]{
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

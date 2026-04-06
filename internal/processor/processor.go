// Package processor coordinates command routing and execution lifecycle for Asynx.
//
// Processor[T] routes incoming commands to shards via consistent hashing,
// manages graceful shutdown, and exposes Send/SendWait/Shutdown interfaces.
//   - router     — FNV-1a hash-based consistent shard selection
//   - pool       — Shard-based worker pool for concurrent command execution
//   - executor   — Passed to pool; executes Load→Validate→Write→Publish pipeline
//
// All command execution is non-blocking via channels. Send and SendWait block until either
// the command completes, context cancels, or the queue is full. Send publishes events
// asynchronously; SendWait publishes synchronously. Shutdown drains in-flight work before closing the bus.
package processor

import (
	"context"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/processor/queue"
	asynxmd "github.com/char2cs/asynx/models"
)

type Processor[T any] struct {
	pool          *pool.ShardPool[T]
	router        *queue.Router
	executor      *exec.CommandExecutor[T]
	bus           asynxmd.Bus[T]
	shuttingDown  atomic.Bool
	onSendPending func()
}

type ProcessorOpt[T any] func(*processorConfig)

type processorConfig struct {
	shards          int
	queueDepth      int
	workersPerShard int
}

func WithShards[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig) {
		if count > 0 {
			cfg.shards = count
		}
	}
}

func WithQueueDepth[T any](depth int) ProcessorOpt[T] {
	return func(cfg *processorConfig) {
		if depth >= 0 {
			cfg.queueDepth = depth
		}
	}
}

func WithWorkersPerShard[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig) {
		if count > 0 {
			cfg.workersPerShard = count
		}
	}
}

func New[T any](
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...ProcessorOpt[T],
) *Processor[T] {
	cfg := &processorConfig{
		shards:          8,
		queueDepth:      -1,
		workersPerShard: 8,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.queueDepth < 0 {
		cfg.queueDepth = cfg.workersPerShard
	}

	executor := exec.New(
		es,
		bus,
	)
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
		executor: executor,
		bus:      bus,
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

	return p.closeBus(ctx)
}

func (p *Processor[T]) setShuttingDown() bool {
	return p.shuttingDown.CompareAndSwap(false, true)
}

func (p *Processor[T]) closeBus(ctx context.Context) error {
	if p.bus == nil {
		return nil
	}
	return p.bus.Close(ctx)
}

// ForTesting: WaitPublish blocks until all async event publishes complete.
// Do not call in production code.
func (p *Processor[T]) WaitPublish() {
	p.executor.WaitPublish()
}

// ForTesting: SetOnSendPending sets a callback invoked after a command is
// enqueued but before Send or SendWait blocks waiting for its result.
// Do not call in production code.
func (p *Processor[T]) SetOnSendPending(fn func()) {
	p.onSendPending = fn
}

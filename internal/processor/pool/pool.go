// Package pool implements shard-based concurrent command execution.
//
// ShardPool[T] spawns one dispatcher and workersPerShard workers per shard. A
// shard serializes commands targeting the same aggregate only when
// workersPerShard is 1; with more workers (the default 8) a shard drains its
// queue in parallel, so two commands on the same aggregate may run concurrently.
//   - Shard.dispatchCommands — Reads envelopes off commandChan and hands them
//     to the worker pool via jobQueue
//   - Shard.workerLoop       — Executes commands; sends results on completion
//
// The event version is assigned by the event store on write (optimistic
// concurrency), not by the pool.
//
// Shutdown is two-phase: signal all dispatchers to stop, drain jobQueues until
// workers finish, then close all channels. Uses sync.WaitGroup for coordination.
package pool

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/internal/processor/exec"
)

type ShardPool[T any] struct {
	shards           []*Shard[T]
	numShards        int
	workersPerShard  int
	doneWg           sync.WaitGroup
	dispatcherDoneWg sync.WaitGroup
}

func New[T any](
	executor *exec.CommandExecutor[T],
	numShards int,
	queueDepth int,
	workersPerShard int,
) *ShardPool[T] {
	shards := make([]*Shard[T], numShards)
	for i := range numShards {
		shards[i] = newShard[T](
			i,
			queueDepth,
			workersPerShard,
		)
	}

	pool := &ShardPool[T]{
		shards:          shards,
		numShards:       numShards,
		workersPerShard: workersPerShard,
	}

	pool.start(executor)
	return pool
}

func (p *ShardPool[T]) Shards() []*Shard[T] {
	return p.shards
}

func (p *ShardPool[T]) start(executor *exec.CommandExecutor[T]) {
	for _, shard := range p.shards {
		p.dispatcherDoneWg.Add(1)
		go shard.dispatchCommands(
			&p.dispatcherDoneWg,
		)

		for range p.workersPerShard {
			p.doneWg.Add(1)
			go shard.workerLoop(
				executor,
				&p.doneWg,
			)
		}
	}
}

func (p *ShardPool[T]) Drain(ctx context.Context) error {
	p.signalStop()

	if err := p.waitDispatchers(ctx); err != nil {
		return err
	}

	return p.waitWorkers(ctx)
}

func (p *ShardPool[T]) signalStop() {
	for _, shard := range p.shards {
		shard.signalStop()
	}
}

func (p *ShardPool[T]) waitDispatchers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.dispatcherDoneWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *ShardPool[T]) waitWorkers(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.doneWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

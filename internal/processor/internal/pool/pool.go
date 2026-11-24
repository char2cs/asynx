package pool

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/internal/processor/models"
)

type ShardPool[T any] struct {
	shards           []*Shard[T]
	numShards        int
	workersPerShard  int
	doneWg           sync.WaitGroup
	dispatcherDoneWg sync.WaitGroup
}

func New[T any](
	executor models.Executor[T],
	numShards int,
	queueDepth int,
	workersPerShard int,
) *ShardPool[T] {
	shards := make(
		[]*Shard[T],
		numShards,
	)
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

func (p *ShardPool[T]) start(
	executor models.Executor[T],
) {
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

func (p *ShardPool[T]) Drain(
	ctx context.Context,
) error {
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

func (p *ShardPool[T]) waitDispatchers(
	ctx context.Context,
) error {
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

func (p *ShardPool[T]) waitWorkers(
	ctx context.Context,
) error {
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

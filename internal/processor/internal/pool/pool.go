package pool

import (
	"context"
	"sync"
)

// ShardPool owns a fixed set of shards and the dispatcher and worker goroutines
// that serve them, all started by New and all ended by Drain. The shard count
// never changes, which is what lets the router map an aggregate to its shard
// for the pool's whole life. It is safe for concurrent use.
type ShardPool[T any] struct {
	shards           []*Shard[T]
	numShards        int
	workersPerShard  int
	doneWg           sync.WaitGroup
	dispatcherDoneWg sync.WaitGroup
}

// New starts a pool of numShards shards, each with queueDepth slots of queue
// and served by one dispatcher goroutine and workersPerShard worker goroutines,
// and returns it running. The caller must end it with Drain, or the goroutines
// live for the life of the process. numShards must be positive; the router
// already validates it.
func New[T any](
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

	pool.start()
	return pool
}

// Shards returns the pool's shards, indexed as the router's Route result. The
// slice is fixed for the pool's life and must not be modified.
func (p *ShardPool[T]) Shards() []*Shard[T] {
	return p.shards
}

func (p *ShardPool[T]) start() {
	for _, shard := range p.shards {
		p.startShard(shard)
	}
}

func (p *ShardPool[T]) startShard(
	shard *Shard[T],
) {
	p.dispatcherDoneWg.Add(1)
	go shard.dispatchCommands(
		&p.dispatcherDoneWg,
	)

	for range p.workersPerShard {
		p.doneWg.Add(1)
		go shard.workerLoop(&p.doneWg)
	}
}

// Drain stops every shard, then waits for all dispatchers and then all workers
// to end, so when it returns nil no command is running and no goroutine of the
// pool is left. Envelopes still queued are rejected with ErrShuttingDown, and
// commands already granted a turn are waited for to finish. If ctx ends first
// it returns ctx.Err() and leaves the goroutines to finish by themselves;
// calling Drain again resumes waiting. Callers must ensure no sender enqueues
// after Drain started, as the processor does by waiting for its sends first.
func (p *ShardPool[T]) Drain(
	ctx context.Context,
) error {
	p.signalStop()

	if err := waitGroup(
		ctx,
		&p.dispatcherDoneWg,
	); err != nil {
		return err
	}

	return waitGroup(
		ctx,
		&p.doneWg,
	)
}

func (p *ShardPool[T]) signalStop() {
	for _, shard := range p.shards {
		shard.signalStop()
	}
}

func waitGroup(
	ctx context.Context,
	wg *sync.WaitGroup,
) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

package asynx_test

import (
	"context"
	"sync"
	"testing"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/store"
)

// countConcurrentCancelSuccesses seeds one order and fires `attempts` concurrent
// cancels at it through the public API, returning how many returned nil (i.e.
// how many cancel events were accepted). An order may be cancelled at most once,
// so the correct answer is always 1 — regardless of workersPerShard.
func countConcurrentCancelSuccesses(t *testing.T, workersPerShard, attempts int) int {
	t.Helper()

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 1, QueueDepth: 4096, WorkersPerShard: workersPerShard}).
		Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "X", Total: 100}); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
		start     = make(chan struct{})
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := instance.Send(ctx, mocks.CancelOrderCmd{ID: "X"}); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return successes
}

// The write path enforces optimistic concurrency, so a burst of concurrent
// cancels resolves to exactly one accepted cancel even with many workers per
// shard — the losers get ErrPipelineFailed rather than silently double-committing.
func TestConcurrentCancel_AcceptsExactlyOne(t *testing.T) {
	for _, workers := range []int{1, 8} {
		if got := countConcurrentCancelSuccesses(t, workers, 64); got != 1 {
			t.Errorf("WorkersPerShard=%d: %d cancels accepted, want exactly 1", workers, got)
		}
	}
}

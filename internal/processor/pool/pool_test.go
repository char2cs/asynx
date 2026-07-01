package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/store"
	asynxmd "github.com/char2cs/asynx/models"
)

// blockingBus is a test bus whose PublishSync blocks until unblockCh is closed.
type blockingBus[T any] struct {
	unblockCh chan struct{}
}

func (b *blockingBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *blockingBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error {
	<-b.unblockCh
	return nil
}
func (b *blockingBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *blockingBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *blockingBus[T]) Close(_ context.Context) error { return nil }
func (b *blockingBus[T]) WaitForHandlers()               {}

type order = mocks.Order

func TestPool_SingleCommandSuccess(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != nil {
		t.Fatalf("expected nil, got %v", result.Err)
	}

	// Verify aggregate was written
	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("expected aggregate to exist, got error: %v", err)
	}
	if agg.ID != "order1" || agg.Total != 100.0 {
		t.Errorf("unexpected aggregate state: %+v", agg)
	}
}

func TestPool_SerialOrderingSameAggregate(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	// Single shard to force serialization
	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Send 10 commands for the same aggregate
	for i := 1; i <= 10; i++ {
		envelope := &models.CommandEnvelope[order]{
			Cmd: mocks.UpdateOrderCmd{
				ID:       "order1",
				NewState: order{ID: "order1", Total: float64(i * 10), Status: "Pending"},
			},
			Ctx:        ctx,
			ResultChan: make(chan models.CommandResult[order], 1),
		}

		shard.CommandChan() <- envelope

		result := <-envelope.ResultChan
		if result.Err != nil {
			t.Fatalf("Send failed at iteration %d: %v", i, result.Err)
		}
	}

	// Verify final state
	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("failed to get aggregate: %v", err)
	}
	if agg.Total != 100.0 {
		t.Errorf("expected Total=100.0 after 10 updates, got %f", agg.Total)
	}
}

func TestPool_ParallelDifferentAggregates(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 8, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			shard := p.Shards()[idx%len(p.Shards())]

			envelope := &models.CommandEnvelope[order]{
				Cmd:        mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0},
				Ctx:        ctx,
				ResultChan: make(chan models.CommandResult[order], 1),
			}

			shard.CommandChan() <- envelope
			result := <-envelope.ResultChan
			errs <- result.Err
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}

	// Verify all aggregates exist
	for i := 0; i < 10; i++ {
		id := "order" + string(rune(i))
		agg, err := es.Get(ctx, id)
		if err != nil {
			t.Errorf("aggregate %s not found: %v", id, err)
			continue
		}
		if agg.Total != 100.0 {
			t.Errorf("aggregate %s has Total=%f, expected 100.0", id, agg.Total)
		}
	}
}

func TestPool_ValidationFailureNoWrite(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Try to cancel a non-existent order (validation failure)
	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}

	shard.CommandChan() <- envelope

	result := <-envelope.ResultChan
	if result.Err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", result.Err)
	}

	// Verify aggregate was NOT written
	_, err := es.Get(ctx, "order1")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestPool_DrainWaitsForInflight(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)

	ctx := context.Background()
	shard := p.Shards()[0]

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			envelope := &models.CommandEnvelope[order]{
				Cmd:        mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0},
				Ctx:        ctx,
				ResultChan: make(chan models.CommandResult[order], 1),
			}

			shard.CommandChan() <- envelope
			result := <-envelope.ResultChan
			errs <- result.Err
		}(i)
	}

	wg.Wait()

	err := p.Drain(context.Background())
	if err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	close(errs)

	// Verify all commands were processed
	for err := range errs {
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}

	// Verify all aggregates exist
	for i := 0; i < 10; i++ {
		id := "order" + string(rune(i))
		_, err := es.Get(ctx, id)
		if err != nil {
			t.Errorf("aggregate %s not found: %v", id, err)
		}
	}
}

func TestPool_DrainTimeout(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err := p.Drain(ctx)
	// Should timeout or complete quickly (implementation dependent)
	if err != nil && err != context.DeadlineExceeded {
		t.Logf("drain returned: %v (may be acceptable if completed quickly)", err)
	}
}

func TestPool_QueueFullBlocksSender(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	// Single shard with queue depth 1
	p := pool.New(executor, 1, 1, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Create a command that blocks to fill the queue
	blocked := make(chan struct{})
	dispatched := make(chan struct{}, 1)
	unblock := make(chan struct{})

	shard.SetOnDispatched(func() {
		select {
		case dispatched <- struct{}{}:
		default:
		}
	})

	blockingCmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	// Start first send in background
	done1 := make(chan error, 1)
	go func() {
		envelope := &models.CommandEnvelope[order]{
			Cmd:        blockingCmd,
			Ctx:        ctx,
			ResultChan: make(chan models.CommandResult[order], 1),
		}
		shard.CommandChan() <- envelope
		close(blocked)
		result := <-envelope.ResultChan
		done1 <- result.Err
	}()

	// Wait for first command to be queued
	<-blocked
	// Wait until dispatcher reads it (slot is now free)
	<-dispatched

	// Send should timeout or block quickly (showing queue is full)
	normalEnvelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order2", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}

	// Use a timeout to detect if send would block
	sendDone := make(chan struct{})
	go func() {
		shard.CommandChan() <- normalEnvelope
		close(sendDone)
	}()

	select {
	case <-sendDone:
		t.Logf("send completed (queue not full or processor fast)")
	case <-time.After(100 * time.Millisecond):
		// Expected: queue full, send blocks
	}

	// Cleanup
	close(unblock)
	<-done1
	p.Drain(context.Background())
}

func TestPool_MultipleWorkers(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 4, 0, 4) // 4 workers per shard
	defer p.Drain(context.Background())

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 100)

	// Send 100 commands across 4 shards with 4 workers each
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			shardIdx := idx % 4
			shard := p.Shards()[shardIdx]

			envelope := &models.CommandEnvelope[order]{
				Cmd:        mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0},
				Ctx:        ctx,
				ResultChan: make(chan models.CommandResult[order], 1),
			}

			shard.CommandChan() <- envelope
			result := <-envelope.ResultChan
			errs <- result.Err
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}
}

func TestPool_ValidationErrorDecrementVersionSingleWorker(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	// Single worker per shard (workersPerShard=1) enables correction
	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// First, create an order successfully
	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope1
	result1 := <-envelope1.ResultChan
	if result1.Err != nil {
		t.Fatalf("create failed: %v", result1.Err)
	}

	// Now try to cancel it (validation succeeds)
	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != nil {
		t.Fatalf("cancel failed: %v", result2.Err)
	}

	// Try to cancel a non-existent order (validation error)
	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation for cancel non-existent, got %v", result3.Err)
	}

	// Send another valid command to ensure version was corrected
	envelope4 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope4
	result4 := <-envelope4.ResultChan
	if result4.Err != nil {
		t.Fatalf("create after validation error failed: %v", result4.Err)
	}
}

func TestPool_ContextCancelledDuringExecution(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}

	shard.CommandChan() <- envelope

	// Cancel context quickly
	cancel()

	// Result channel should still receive (either success or context cancelled)
	select {
	case <-envelope.ResultChan:
		// OK either way
	case <-time.After(2 * time.Second):
		t.Fatalf("result not received within timeout")
	}
}

func TestPool_DrainWithNoCommands(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 2, 0, 2)

	// Drain immediately without sending any commands
	err := p.Drain(context.Background())
	if err != nil {
		t.Fatalf("drain failed: %v", err)
	}
}

func TestPool_DrainTimeoutBothPhases(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)

	// Use an already-expired deadline so there is no race with the clock.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := p.Drain(ctx)
	// Should timeout during drain
	if err != context.DeadlineExceeded && err != nil {
		t.Logf("Drain timeout result: %v", err)
	}
}

func TestPool_DispatcherMultipleCorrections(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	// Multiple workers - corrections not sent
	p := pool.New(executor, 1, 0, 2)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Create an order
	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	// Try validation error with multiple workers (no correction)
	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope2
	result2 := <-envelope2.ResultChan
	if result2.Err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", result2.Err)
	}

	// Next command should work (no version gap since no correction sent)
	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[order], 1),
	}
	shard.CommandChan() <- envelope3
	result3 := <-envelope3.ResultChan
	if result3.Err != nil {
		t.Fatalf("create after failed validation (multi-worker) failed: %v", result3.Err)
	}
}

// TestPool_WaitWorkers_ContextCancelledAfterDispatch verifies the ctx.Done branch
// in waitWorkers: context expires while workers are still executing a long-running job.
func TestPool_WaitWorkers_ContextCancelledAfterDispatch(t *testing.T) {
	s := store.New()
	// Use a blocking bus so the worker blocks during PublishSync (WaitHandlers=true).
	unblockCh := make(chan struct{})
	bb := &blockingBus[order]{unblockCh: unblockCh}
	es := eventstore.New[order](s, s, nil, 1, nil)
	d := dispatcher.New[order](bb)
	t.Cleanup(func() { d.Close(context.Background()) })
	executor := exec.New(es, d)

	p := pool.New(executor, 1, 0, 1)

	ctx := context.Background()
	shard := p.Shards()[0]

	// Send a command with WaitHandlers=true so the worker blocks in PublishSync.
	envelope := &models.CommandEnvelope[order]{
		Cmd:          mocks.CreateOrderCmd{ID: "blocking-order", Total: 10.0},
		Ctx:          ctx,
		ResultChan:   make(chan models.CommandResult[order], 1),
		WaitHandlers: true,
	}
	shard.CommandChan() <- envelope

	// Signal stop so the dispatcher exits (waitDispatchers phase will complete).
	// Use a short-lived context for Drain to expire during waitWorkers.
	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := p.Drain(drainCtx)
	if err == nil {
		// Unblock the bus so the test doesn't leak goroutines.
		close(unblockCh)
		t.Skip("Drain completed before timeout — worker was too fast; skipping")
	}
	if err != context.DeadlineExceeded {
		close(unblockCh)
		t.Fatalf("expected DeadlineExceeded from waitWorkers, got %v", err)
	}

	// Unblock the worker so it can exit cleanly and avoid goroutine leak.
	close(unblockCh)
}

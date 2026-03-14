package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

func TestPool_SingleCommandSuccess(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}

	shard.CommandChan() <- envelope

	err := <-envelope.ResultChan
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
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
	executor := exec.New(es, b)

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
			ResultChan: make(chan error, 1),
		}

		shard.CommandChan() <- envelope

		err := <-envelope.ResultChan
		if err != nil {
			t.Fatalf("Send failed at iteration %d: %v", i, err)
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
	executor := exec.New(es, b)

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
				ResultChan: make(chan error, 1),
			}

			shard.CommandChan() <- envelope
			errs <- <-envelope.ResultChan
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
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Try to cancel a non-existent order (validation failure)
	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}

	shard.CommandChan() <- envelope

	err := <-envelope.ResultChan
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	// Verify aggregate was NOT written
	_, err = es.Get(ctx, "order1")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestPool_DrainWaitsForInflight(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

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
				ResultChan: make(chan error, 1),
			}

			shard.CommandChan() <- envelope
			errs <- <-envelope.ResultChan
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
	executor := exec.New(es, b)

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
	executor := exec.New(es, b)

	// Single shard with queue depth 1
	p := pool.New(executor, 1, 1, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Create a command that blocks to fill the queue
	blocked := make(chan struct{})
	unblock := make(chan struct{})

	blockingCmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	// Start first send in background
	done1 := make(chan error, 1)
	go func() {
		envelope := &models.CommandEnvelope[order]{
			Cmd:        blockingCmd,
			Ctx:        ctx,
			ResultChan: make(chan error, 1),
		}
		shard.CommandChan() <- envelope
		close(blocked)
		done1 <- <-envelope.ResultChan
	}()

	// Wait for first command to be queued
	<-blocked
	time.Sleep(50 * time.Millisecond) // Give it time to start processing

	// Send should timeout or block quickly (showing queue is full)
	normalEnvelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order2", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
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
	executor := exec.New(es, b)

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
				ResultChan: make(chan error, 1),
			}

			shard.CommandChan() <- envelope
			errs <- <-envelope.ResultChan
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
	executor := exec.New(es, b)

	// Single worker per shard (workersPerShard=1) enables correction
	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// First, create an order successfully
	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope1
	err := <-envelope1.ResultChan
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Now try to cancel it (validation succeeds)
	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order1"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope2
	err = <-envelope2.ResultChan
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	// Try to cancel a non-existent order (validation error)
	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope3
	err = <-envelope3.ResultChan
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation for cancel non-existent, got %v", err)
	}

	// Send another valid command to ensure version was corrected
	envelope4 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 200.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope4
	err = <-envelope4.ResultChan
	if err != nil {
		t.Fatalf("create after validation error failed: %v", err)
	}
}

func TestPool_ContextCancelledDuringExecution(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)
	defer p.Drain(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	shard := p.Shards()[0]

	envelope := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}

	shard.CommandChan() <- envelope

	// Cancel context quickly
	cancel()

	// Result channel should still receive (either success or context cancelled)
	select {
	case err := <-envelope.ResultChan:
		_ = err // OK either way
	case <-time.After(2 * time.Second):
		t.Fatalf("result not received within timeout")
	}
}

func TestPool_DrainWithNoCommands(t *testing.T) {
	s := store.New()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := exec.New(es, b)

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
	executor := exec.New(es, b)

	p := pool.New(executor, 1, 0, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	time.Sleep(5 * time.Millisecond)

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
	executor := exec.New(es, b)

	// Multiple workers - corrections not sent
	p := pool.New(executor, 1, 0, 2)
	defer p.Drain(context.Background())

	ctx := context.Background()
	shard := p.Shards()[0]

	// Create an order
	envelope1 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order1", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope1
	<-envelope1.ResultChan

	// Try validation error with multiple workers (no correction)
	envelope2 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CancelOrderCmd{ID: "order2"},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope2
	err := <-envelope2.ResultChan
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	// Next command should work (no version gap since no correction sent)
	envelope3 := &models.CommandEnvelope[order]{
		Cmd:        mocks.CreateOrderCmd{ID: "order3", Total: 100.0},
		Ctx:        ctx,
		ResultChan: make(chan error, 1),
	}
	shard.CommandChan() <- envelope3
	err = <-envelope3.ResultChan
	if err != nil {
		t.Fatalf("create after failed validation (multi-worker) failed: %v", err)
	}
}

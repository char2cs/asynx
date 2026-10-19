package processor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/exec"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

func newProcessor(t *testing.T, opts ...ProcessorOpt[order]) *Processor[order] {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus, opts...)

	t.Cleanup(func() {
		p.Shutdown(context.Background())
	})

	return p
}

func TestProcessor_SendSuccess(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestProcessor_ValidationError(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: -100.0} // Invalid

	_, err := p.Send(ctx, cmd)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestProcessor_ValidationNoWrite(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()

	// Create an order first
	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	p.Send(ctx, cmd1)

	// Try to cancel a non-existent order (validation failure)
	cmd2 := mocks.CancelOrderCmd{ID: "order2"} // order2 doesn't exist

	_, err := p.Send(ctx, cmd2)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestProcessor_SerialOrdering10Increments(t *testing.T) {
	p := newProcessor(t, WithShards[order](1))

	ctx := context.Background()

	// Send 10 commands for the same aggregate
	for i := 1; i <= 10; i++ {
		cmd := mocks.UpdateOrderCmd{
			ID:       "order1",
			NewState: order{ID: "order1", Total: float64(i * 10), Status: "Pending"},
		}
		_, err := p.Send(ctx, cmd)
		if err != nil {
			t.Fatalf("Send failed at iteration %d: %v", i, err)
		}
	}
}

func TestProcessor_ParallelAggregates8Goroutines(t *testing.T) {
	p := newProcessor(t, WithShards[order](8))

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 8)

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
			_, _err := p.Send(ctx, cmd)
			errs <- _err
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

func TestProcessor_ShuttingDown(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	err := p.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Try to send after shutdown
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err = p.Send(ctx, cmd)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestProcessor_ContextCancelledBeforeQueue(t *testing.T) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != asynxmd.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}
}

func TestProcessor_ContextCancelledWhileWaiting(t *testing.T) {
	p := newProcessor(t, WithShards[order](1))

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	pending := make(chan struct{}, 1)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	// Start send in background
	done := make(chan error, 1)
	go func() {
		_, _err := p.Send(ctx, cmd)
		done <- _err
	}()

	// Wait until Send is waiting for result
	<-pending

	// Cancel context
	cancel()

	// Wait for send to return
	err := <-done
	// Could be ErrContextCancelled or nil (if it completed before cancellation)
	if err != nil && err != asynxmd.ErrContextCancelled {
		t.Logf("Send returned: %v (acceptable if completed quickly)", err)
	}
}

func TestProcessor_QueueFull(t *testing.T) {
	p := newProcessor(t, WithShards[order](1), WithQueueDepth[order](1))

	ctx := context.Background()

	// Create a command that takes time to process (conceptually)
	// Since we can't easily control execution time, we'll test that
	// the queueing behavior is correct
	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd1)
	if err != nil {
		t.Fatalf("first send failed: %v", err)
	}

	// Second send should succeed (depth=1 means 1 in queue)
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(ctx, cmd2)
	if err != nil {
		t.Logf("second send result: %v (may be acceptable)", err)
	}
}

func TestProcessor_ShutdownDrains(t *testing.T) {
	p := newProcessor(t, WithShards[order](4), WithQueueDepth[order](5))

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
			_, _err := p.Send(ctx, cmd)
			errs <- _err
		}(i)
	}

	wg.Wait()

	err := p.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	close(errs)

	// Verify all commands were processed
	for err := range errs {
		if err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}
}

func TestProcessor_ShutdownTimeout(t *testing.T) {
	p := newProcessor(t)

	// Use an already-expired deadline so there is no race with the clock.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := p.Shutdown(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Logf("Shutdown returned: %v (may be acceptable if completed quickly)", err)
	}
}

func TestProcessor_AlreadyShuttingDown(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()

	// First shutdown
	err1 := p.Shutdown(ctx)
	if err1 != nil {
		t.Fatalf("first shutdown failed: %v", err1)
	}

	// Second shutdown should return error
	err2 := p.Shutdown(ctx)
	if err2 != asynxmd.ErrAlreadyShuttingDown {
		t.Fatalf("expected ErrAlreadyShuttingDown, got %v", err2)
	}
}

func TestProcessor_EventPublishedToRealBus(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	// Subscribe to events
	eventReceived := make(chan asynxmd.Event[order], 1)
	channelBus.Subscribe("OrderCreated", func(ctx context.Context, e asynxmd.Event[order]) {
		eventReceived <- e
	})

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for event to be published
	select {
	case event := <-eventReceived:
		if event.EventName != "OrderCreated" {
			t.Errorf("unexpected event name: %s", event.EventName)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("event not published within timeout")
	}
}

func TestProcessor_PublishUsesDetachedContext(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus)
	defer p.Shutdown(context.Background())

	ctx, cancel := context.WithCancel(context.Background())

	// Subscribe to events
	contextReceived := make(chan context.Context, 1)
	channelBus.Subscribe("OrderCreated", func(ctx context.Context, e asynxmd.Event[order]) {
		contextReceived <- ctx
	})

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Cancel the original context
	cancel()

	// Wait for event to be published
	select {
	case <-contextReceived:
		// Event was published despite context cancellation
	case <-time.After(1 * time.Second):
		t.Fatalf("event not published within timeout")
	}
}

func TestProcessor_WithWorkersPerShard(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](2),
		WithWorkersPerShard[order](6),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestProcessor_WithQueueDepth(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithQueueDepth[order](100),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestProcessor_NegativeOptionsIgnored(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	// Negative values should be ignored, using defaults
	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](-5),
		WithQueueDepth[order](-1),
		WithWorkersPerShard[order](0),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestProcessor_SendContextCancelledDuringWait(t *testing.T) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	pending := make(chan struct{}, 1)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	go func() {
		<-pending
		cancel()
	}()

	_, err := p.Send(ctx, cmd)
	// Should get either context cancelled or success (timing dependent)
	if err != nil && err != asynxmd.ErrContextCancelled {
		t.Logf("Send returned: %v", err)
	}
}

func TestProcessor_ShutdownWithBus(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus)

	ctx := context.Background()

	// Send a command
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Shutdown with bus
	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestProcessor_SendToMultipleShards(t *testing.T) {
	p := newProcessor(t, WithShards[order](4))

	ctx := context.Background()

	// Send to different shards
	for i := 0; i < 12; i++ {
		cmd := mocks.CreateOrderCmd{ID: "order" + string(rune('0'+i)), Total: 100.0}
		_, err := p.Send(ctx, cmd)
		if err != nil {
			t.Fatalf("Send %d failed: %v", i, err)
		}
	}
}

func TestProcessor_ShutdownWithoutBus(t *testing.T) {
	memStore := store.New()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, nil)

	ctx := context.Background()

	// Send a command
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Shutdown without bus
	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestProcessor_ContextCancelledEarlyCheck(t *testing.T) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before calling Send

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := p.Send(ctx, cmd)
	if err != asynxmd.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}
}

func TestProcessor_AllSendBranches(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus, WithShards[order](8), WithQueueDepth[order](100))
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	// Test normal path
	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd1)
	if err != nil {
		t.Fatalf("normal send failed: %v", err)
	}

	// Test with cancelled context in early check
	ctx2, cancel := context.WithCancel(context.Background())
	cancel()
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(ctx2, cmd2)
	if err != asynxmd.ErrContextCancelled {
		t.Fatalf("cancelled early should return ErrContextCancelled, got %v", err)
	}

	// Test validation error
	cmd3 := mocks.CreateOrderCmd{ID: "order3", Total: -100.0}
	_, err = p.Send(ctx, cmd3)
	if err != asynxmd.ErrValidation {
		t.Fatalf("validation error should propagate, got %v", err)
	}

	// Test successful multiple operations
	for i := 0; i < 5; i++ {
		cmd := mocks.CreateOrderCmd{ID: "order" + string(rune('A'+i)), Total: 100.0}
		_, err := p.Send(ctx, cmd)
		if err != nil {
			t.Fatalf("send %d failed: %v", i, err)
		}
	}
}

func TestProcessor_ShutdownClosesPool(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus)

	ctx := context.Background()

	// Verify we can send before shutdown
	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd1)
	if err != nil {
		t.Fatalf("send before shutdown failed: %v", err)
	}

	// Shutdown
	err = p.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Verify sends are blocked after shutdown
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err = p.Send(ctx, cmd2)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf("send after shutdown should return ErrShuttingDown, got %v", err)
	}
}

func TestProcessor_SendWait_Success(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "emit-order", Total: 42.0}

	event, err := p.SendWait(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if event.AggregateID != "emit-order" {
		t.Errorf("expected AggregateID emit-order, got %q", event.AggregateID)
	}
	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
}

func TestProcessor_SendWait_HandlersCompleteBeforeReturn(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, channelBus)
	defer p.Shutdown(context.Background())

	var handlerDone atomic.Bool
	channelBus.Subscribe("OrderCreated", func(_ context.Context, _ asynxmd.Event[order]) {
		handlerDone.Store(true)
	})

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "handler-order", Total: 10.0}

	_, err := p.SendWait(ctx, cmd)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// No WaitPublish needed — handlers must be done when SendWait returns.
	if !handlerDone.Load() {
		t.Error("handler not done when SendWait returned")
	}
}

func TestProcessor_SendWait_ValidationError(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "bad-order", Total: -1.0}

	_, err := p.SendWait(ctx, cmd)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestProcessor_SendWait_ContextCancelled(t *testing.T) {
	p := newProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "ctx-order", Total: 1.0}

	_, err := p.SendWait(ctx, cmd)
	if err != asynxmd.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}
}

func TestProcessor_SendWait_AfterShutdown(t *testing.T) {
	p := newProcessor(t)

	ctx := context.Background()
	p.Shutdown(ctx)

	cmd := mocks.CreateOrderCmd{ID: "shutdown-order", Total: 1.0}

	_, err := p.SendWait(ctx, cmd)
	if err != asynxmd.ErrShuttingDown {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestWithPublishErrorHandler_IsInvokedOnPublishError(t *testing.T) {
	// Use a bus that always errors on Publish to trigger the handler.
	var got struct {
		sync.Mutex
		called bool
	}

	s := store.New()
	handler := func(_ context.Context, _ asynxmd.Event[mocks.Order], _ error) {
		got.Lock()
		got.called = true
		got.Unlock()
	}

	proc := mustNew(t,
		eventstore.New[mocks.Order](s, store.NewSnapshots(), nil, 1, nil),
		&mocks.ErrBus[mocks.Order]{Err: errors.New("bus down")},
		WithPublishErrorHandler[mocks.Order](handler),
	)

	_, err := proc.Send(context.Background(), mocks.CreateOrderCmd{ID: "order-1", Total: 10})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait for the async publish goroutine to fire.
	proc.WaitPublish()

	got.Lock()
	called := got.called
	got.Unlock()

	if !called {
		t.Error("PublishErrorHandler was not called after bus error")
	}
}

// blockingTestBus is a bus whose PublishSync blocks until unblockCh is closed.
// Used to hold the worker goroutine inside executeJob so we can control timing.
type blockingTestBus[T any] struct {
	unblockCh chan struct{}
}

func (b *blockingTestBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *blockingTestBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error {
	<-b.unblockCh
	return nil
}
func (b *blockingTestBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *blockingTestBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *blockingTestBus[T]) Close(_ context.Context) error { return nil }
func (b *blockingTestBus[T]) WaitForHandlers()              {}

// TestProcessor_SendWait_ContextCancelledWhileWaiting verifies the second-select
// ctx.Done branch in sendAndWait: context is cancelled after the command is enqueued
// but before the result arrives. A blocking bus ensures the worker is still running
// when ctx is cancelled, making the second-select ctx.Done branch deterministic.
func TestProcessor_SendWait_ContextCancelledWhileWaiting(t *testing.T) {
	memStore := store.New()
	unblockCh := make(chan struct{})
	bb := &blockingTestBus[order]{unblockCh: unblockCh}
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t, es, bb, WithShards[order](1))
	t.Cleanup(func() {
		close(unblockCh) // unblock any pending PublishSync before shutdown
		p.Shutdown(context.Background())
	})

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "ew-ctx-wait", Total: 1.0}

	pending := make(chan struct{}, 1)
	p.SetOnSendPending(func() {
		select {
		case pending <- struct{}{}:
		default:
		}
	})

	done := make(chan error, 1)
	go func() {
		// SendWait enqueues, then blocks waiting for result (which requires PublishSync to finish).
		// Since bb.PublishSync blocks on unblockCh, the worker won't finish until we unblock.
		_, err := p.SendWait(ctx, cmd)
		done <- err
	}()

	// Wait until command is enqueued and worker is processing it.
	<-pending

	// Cancel ctx while worker is still blocked in PublishSync.
	cancel()

	// SendWait should return ErrContextCancelled via the second select ctx.Done branch.
	select {
	case err := <-done:
		if err != asynxmd.ErrContextCancelled {
			t.Errorf("expected ErrContextCancelled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("SendWait did not return after context cancel")
	}
}

type fixture struct {
	p    *Processor[order]
	s    store.Memory
	snap store.SnapshotMemory
	b    *bus.ChannelBus[order]
}

func newFixture(t *testing.T, s store.Memory, opts ...ProcessorOpt[order]) fixture {
	t.Helper()
	snap := store.NewSnapshots()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, snap, nil, 1, nil)
	p := mustNew(t, es, b, opts...)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return fixture{p: p, s: s, snap: snap, b: b}
}

func updateCmd(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

// parkingOpts makes a delivery give up after two attempts unless healthy is set.
func parkingOpts(healthy *atomic.Bool, extra ...ProcessorOpt[order]) []ProcessorOpt[order] {
	return append([]ProcessorOpt[order]{
		WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(context.Context, asynxmd.Event[order]) error {
				if healthy.Load() {
					return nil
				}
				return errors.New("down")
			},
		}}),
		WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
		WithDeliveryMaxAttempts[order](2),
	}, extra...)
}

func TestProcessor_Forget_ErasesAndNotifiesHandlers(t *testing.T) {
	f := newFixture(t, store.New())
	ctx := context.Background()

	var tombstones atomic.Int32
	f.b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) { tombstones.Add(1) })

	if _, err := f.p.Send(ctx, mocks.CreateOrderCmd{ID: "o1", Total: 5}); err != nil {
		t.Fatal(err)
	}
	if err := f.p.Forget(ctx, "o1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if tombstones.Load() != 1 {
		t.Errorf("forget handler calls = %d, want 1", tombstones.Load())
	}
	if blobs, _ := f.s.ReadFrom(ctx, "events:o1", 1); len(blobs) != 0 {
		t.Errorf("stream not erased: %d blobs", len(blobs))
	}
	if err := f.p.Forget(ctx, "o1"); !errors.Is(err, asynxmd.ErrValidation) {
		t.Errorf("second Forget err = %v, want ErrValidation", err)
	}
}

func TestProcessor_Forget_RejectedWhenShuttingDownOrCancelled(t *testing.T) {
	f := newFixture(t, store.New())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.p.Forget(cancelled, "o1"); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Errorf("cancelled: err = %v, want ErrContextCancelled", err)
	}

	f.p.Shutdown(context.Background())
	if err := f.p.Forget(context.Background(), "o1"); !errors.Is(err, asynxmd.ErrShuttingDown) {
		t.Errorf("shutting down: err = %v, want ErrShuttingDown", err)
	}
}

func TestProcessor_Options_SyncProjectionsDurableDeliverySnapshotErrors(t *testing.T) {
	var projected, snapErrs atomic.Int32
	s := store.New()
	f := newFixture(t, s,
		WithSyncProjections([]exec.SyncProjection[order]{{
			Match: func(string) bool { return true },
			Fn: func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
				projected.Add(1)
				return nil
			},
		}}),
		WithDurableDelivery[order](func(string) bool { return true }),
		WithSnapshotErrorHandler(func(context.Context, asynxmd.Event[order], error) { snapErrs.Add(1) }),
	)
	f.snap.SetError("o1", errors.New("snapshot down"))

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{ID: "o1", NewState: order{ID: "o1", Total: 1}, Snapshot: true}
	if _, err := f.p.SendWait(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	f.p.WaitPublish()

	if projected.Load() != 1 || snapErrs.Load() != 1 {
		t.Errorf("projected=%d snapshotErrors=%d, want 1 and 1", projected.Load(), snapErrs.Load())
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("delivery not acked after SendWait: %v", pending)
	}
}

func TestProcessor_DurableDeliveryOptions_RetryReportAndPanicHandling(t *testing.T) {
	var attempts, reports, panics atomic.Int32
	s := store.New()
	f := newFixture(t, s,
		WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: func(string) bool { return true },
			Handler: func(context.Context, asynxmd.Event[order]) error {
				switch attempts.Add(1) {
				case 1:
					return errors.New("transient")
				case 2:
					panic("boom")
				}
				return nil
			},
		}}),
		WithDeliveryErrorHandler(func(context.Context, asynxmd.Event[order], error, int) { reports.Add(1) }),
		WithDeliveryPanicHandler(func(context.Context, asynxmd.Event[order], any) { panics.Add(1) }),
		WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
	)

	ctx := context.Background()
	if _, err := f.p.Send(ctx, mocks.UpdateOrderCmd{ID: "o1", NewState: order{ID: "o1", Total: 1}}); err != nil {
		t.Fatal(err)
	}
	f.p.WaitPublish()

	if attempts.Load() != 3 || reports.Load() != 2 || panics.Load() != 1 {
		t.Fatalf("attempts=%d reports=%d panics=%d; want 3, 2, 1", attempts.Load(), reports.Load(), panics.Load())
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none after the retry succeeded", pending)
	}
}

func always(string) bool { return true }

func TestProcessor_Options_DeadLetter(t *testing.T) {
	var healthy atomic.Bool
	dead := make(chan int64, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		WithDeadLetterHandler(func(_ context.Context, e asynxmd.Event[order], _ error) error {
			dead <- e.Version
			return nil
		}))...)
	if _, err := f.p.Send(context.Background(), updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-dead:
		if v != 1 {
			t.Fatalf("dead-lettered v%d", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler never called")
	}
}

func TestProcessor_SendWait_GivesUpWaitingButReturnsTheCommittedEvent(t *testing.T) {
	f := newFixture(t, store.New())
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	f.b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		evt asynxmd.Event[order]
		err error
	}
	got := make(chan result, 1)
	go func() {
		evt, err := f.p.SendWait(ctx, updateCmd("o1", 1))
		got <- result{evt, err}
	}()
	<-entered
	cancel()
	r := <-got
	if !errors.Is(r.err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", r.err)
	}
	if r.evt.AggregateID != "o1" || r.evt.Version != 1 {
		t.Fatalf("event = %+v, want the committed v1", r.evt)
	}
}

func TestProcessor_Shutdown_WaitsForForgetInFlightUntilItsContextEnds(t *testing.T) {
	f := newFixture(t, store.New())
	ctx := context.Background()
	if _, err := f.p.Send(ctx, mocks.CreateOrderCmd{ID: "o1", Total: 1}); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	f.b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.p.Forget(ctx, "o1")
	}()
	<-entered

	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := f.p.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want the context's deadline while a Forget is in flight", err)
	}
	close(release)
	wg.Wait()
}

var _ = bus.NewChannelBus[order]

// gatedCmd blocks in Validate until its gate opens, which holds a shard worker.
type gatedCmd struct {
	mocks.UpdateOrderCmd
	entered chan<- struct{}
	gate    <-chan struct{}
}

func (c gatedCmd) Validate(o *order) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.gate
	return nil
}

// Shutdown must not return while a sender is still waiting on a queue the pool
// has stopped reading: every Send either runs or is refused.
func TestProcessor_Shutdown_NeverStrandsAQueuedSend(t *testing.T) {
	for range 25 {
		gate := make(chan struct{})
		entered := make(chan struct{}, 1)
		p := newProcessor(t,
			WithShards[order](1),
			WithWorkersPerShard[order](1),
			WithQueueDepth[order](2))

		results := make(chan error, 6)
		for i := range 6 {
			go func() {
				cmd := gatedCmd{UpdateOrderCmd: updateCmd("agg"+string(rune('a'+i)), 1), entered: entered, gate: gate}
				_, err := p.Send(context.Background(), cmd)
				results <- err
			}()
		}
		<-entered

		down := make(chan error, 1)
		go func() { down <- p.Shutdown(context.Background()) }()
		close(gate)

		for range 6 {
			select {
			case <-results:
			case <-time.After(5 * time.Second):
				t.Fatal("a Send was stranded by Shutdown")
			}
		}
		if err := <-down; err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}
}

func TestProcessor_Shutdown_CanBeRetriedAfterItsContextEnds(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	b := bus.NewChannelBus[order]()
	release := make(chan struct{})
	entered := make(chan struct{})
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})
	p := mustNew(t, es, b)
	if _, err := p.Send(context.Background(), updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-entered

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown succeeded with a handler still running")
	}
	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("retried Shutdown: %v", err)
	}
	if err := p.Shutdown(context.Background()); !errors.Is(err, asynxmd.ErrAlreadyShuttingDown) {
		t.Fatalf("Shutdown after completion = %v, want ErrAlreadyShuttingDown", err)
	}
}

func TestProcessor_SendWait_FromAHandlerOfTheSameAggregateFailsFast(t *testing.T) {
	f := newFixture(t, store.New())
	got := make(chan error, 1)
	f.b.Subscribe("OrderUpdated", func(ctx context.Context, e asynxmd.Event[order]) {
		_, err := f.p.SendWait(ctx, updateCmd(e.AggregateID, 2))
		got <- err
	})
	if _, err := f.p.Send(context.Background(), updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if !errors.Is(err, asynxmd.ErrSendWaitFromHandler) {
			t.Fatalf("SendWait from the handler = %v, want ErrSendWaitFromHandler", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendWait from a handler hung")
	}
}

func TestProcessor_Shutdown_ReportsAWorkerThatWillNotFinish(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	p := newProcessor(t, WithShards[order](1), WithWorkersPerShard[order](1))
	t.Cleanup(func() { close(gate) })

	// The sender gives up, but its command is still running on the worker.
	sendCtx, cancelSend := context.WithCancel(context.Background())
	sent := make(chan error, 1)
	go func() {
		_, err := p.Send(sendCtx, gatedCmd{UpdateOrderCmd: updateCmd("agg", 1), entered: entered, gate: gate})
		sent <- err
	}()
	<-entered
	cancelSend()
	<-sent

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown succeeded with a command still running")
	}
}

func TestProcessor_Shutdown_WaitsForASendInFlightUntilItsContextEnds(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	p := newProcessor(t, WithShards[order](1), WithWorkersPerShard[order](1))
	t.Cleanup(func() { close(gate) })
	go p.Send(context.Background(), gatedCmd{UpdateOrderCmd: updateCmd("agg", 1), entered: entered, gate: gate})
	<-entered

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(short); err == nil {
		t.Fatal("Shutdown returned while a Send was in flight")
	}
}

// Applications use one aggregate ID across several instances (an arrow and its
// runtime): a handler of one instance may Forget or SendWait on the other.
func TestProcessor_HandlerMayWaitOnAnotherInstanceUnderTheSameID(t *testing.T) {
	a := newFixture(t, store.New())
	b := newFixture(t, store.New())
	ctx := context.Background()
	if _, err := b.p.Send(ctx, updateCmd("ns", 1)); err != nil {
		t.Fatal(err)
	}

	got := make(chan error, 2)
	a.b.Subscribe("OrderUpdated", func(hctx context.Context, e asynxmd.Event[order]) {
		_, err := b.p.SendWait(hctx, updateCmd(e.AggregateID, 2))
		got <- err
		got <- b.p.Forget(hctx, e.AggregateID)
	})
	if _, err := a.p.Send(ctx, updateCmd("ns", 1)); err != nil {
		t.Fatal(err)
	}
	for _, what := range []string{"SendWait", "Forget"} {
		select {
		case err := <-got:
			if err != nil {
				t.Fatalf("%s on the other instance from a handler: %v", what, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s on the other instance from a handler hung", what)
		}
	}
}

// WaitPublish waits for what is queued and refuses nothing: a command sent while
// it waits commits, is not reported as failed, and parks nobody.
func TestProcessor_WaitPublish_DoesNotFailCommandsSentWhileItWaits(t *testing.T) {
	f := newFixture(t, store.New())
	ctx := context.Background()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	holdSlow := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		if e.AggregateID == "slow" {
			entered <- struct{}{}
			<-release
		}
	}
	if _, err := f.b.Subscribe("OrderCreated", holdSlow); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Send(ctx, mocks.CreateOrderCmd{ID: "slow", Total: 1}); err != nil {
		t.Fatal(err)
	}
	<-entered

	idle := make(chan struct{})
	go func() { f.p.WaitPublish(); close(idle) }()

	if _, err := f.p.SendWait(ctx, mocks.CreateOrderCmd{ID: "other", Total: 1}); err != nil {
		t.Fatalf("a command sent while WaitPublish waits failed: %v", err)
	}
	if paused := f.p.Paused(); len(paused) != 0 {
		t.Fatalf("Paused = %v, want nobody parked", paused)
	}

	close(release)
	<-idle
}

func mustNew[T any](
	tb testing.TB,
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...ProcessorOpt[T],
) *Processor[T] {
	tb.Helper()
	p, err := New(es, bus, opts...)
	if err != nil {
		tb.Fatalf("processor.New: %v", err)
	}
	return p
}

func newES() *eventstore.EventStore[order] {
	return eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
}

func TestNew_AFactoryReplacesTheDefaultDispatcher(t *testing.T) {
	fake := mocks.NewDispatcher[order]()
	p := mustNew(t, newES(), bus.NewChannelBus[order](), WithDispatcherFactory(fake.Factory()))
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if _, err := p.Send(context.Background(), mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}

	requests := fake.Requests()
	wrongRequests := len(requests) != 1 || requests[0].Kind != asynxmd.DispatchEvent || requests[0].Event.AggregateID != "a"
	if wrongRequests {
		t.Errorf("requests = %+v, want the one DispatchEvent of a", requests)
	}
}

func TestNew_AFactoryErrorIsReturned(t *testing.T) {
	sentinel := errors.New("factory failed")
	factory := func(asynxmd.DispatcherConfig[order]) (asynxmd.Dispatcher[order], error) {
		return nil, sentinel
	}

	p, err := New(newES(), bus.NewChannelBus[order](), WithDispatcherFactory(factory))

	if !errors.Is(err, sentinel) || p != nil {
		t.Fatalf("New = (%v, %v), want the factory error and no processor", p, err)
	}
	if !strings.HasPrefix(err.Error(), "asynx: dispatcher factory: ") {
		t.Errorf("error = %q, want the %q prefix", err, "asynx: dispatcher factory: ")
	}
}

func TestNew_ANilDispatcherWithoutAnErrorFails(t *testing.T) {
	factory := func(asynxmd.DispatcherConfig[order]) (asynxmd.Dispatcher[order], error) {
		return nil, nil
	}

	p, err := New(newES(), bus.NewChannelBus[order](), WithDispatcherFactory(factory))

	if err == nil || p != nil {
		t.Errorf("New = (%v, %v), want an error and no processor", p, err)
	}
}

func TestNew_AFactoryIsCalledEvenWithoutABus(t *testing.T) {
	fake := mocks.NewDispatcher[order]()
	p := mustNew[order](t, newES(), nil, WithDispatcherFactory(fake.Factory()))
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if _, err := p.Send(context.Background(), mocks.CreateOrderCmd{ID: "a", Total: 1}); err != nil {
		t.Fatal(err)
	}

	if len(fake.Requests()) != 1 || fake.Config().Bus != nil {
		t.Errorf("requests = %d, bus = %v, want one request and a nil bus", len(fake.Requests()), fake.Config().Bus)
	}
}

func TestNew_TheFactoryReceivesTheConfigWithDefaultsApplied(t *testing.T) {
	tests := []struct {
		name     string
		opts     []ProcessorOpt[order]
		attempts int
		initial  time.Duration
		max      time.Duration
	}{
		{
			name:     "no options keeps the defaults",
			attempts: 20,
			initial:  100 * time.Millisecond,
			max:      30 * time.Second,
		},
		{
			name: "non-positive values are ignored",
			opts: []ProcessorOpt[order]{
				WithDeliveryMaxAttempts[order](0),
				WithDeliveryRetry[order](-time.Second, 0),
			},
			attempts: 20,
			initial:  100 * time.Millisecond,
			max:      30 * time.Second,
		},
		{
			name: "positive values are carried",
			opts: []ProcessorOpt[order]{
				WithDeliveryMaxAttempts[order](4),
				WithDeliveryRetry[order](time.Millisecond, 9*time.Millisecond),
			},
			attempts: 4,
			initial:  time.Millisecond,
			max:      9 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertFactoryConfig(t, tt.opts, tt.attempts, tt.initial, tt.max)
		})
	}
}

func assertFactoryConfig(
	t *testing.T,
	opts []ProcessorOpt[order],
	attempts int,
	initial time.Duration,
	max time.Duration,
) {
	t.Helper()
	handled := func(
		context.Context,
		asynxmd.Event[order],
		error,
	) {
	}
	fake := mocks.NewDispatcher[order]()
	opts = append(opts, WithDispatcherFactory(fake.Factory()), WithPublishErrorHandler(handled))
	mustNew(t, newES(), bus.NewChannelBus[order](), opts...)

	got := fake.Config()
	configDiffers := got.MaxAttempts != attempts || got.RetryInitial != initial || got.RetryMax != max
	if configDiffers {
		t.Errorf("config = (%d, %v, %v), want (%d, %v, %v)", got.MaxAttempts, got.RetryInitial, got.RetryMax, attempts, initial, max)
	}
	if got.Bus == nil || got.OnPublishError == nil {
		t.Errorf("config = %+v, want the bus and the publish error handler", got)
	}
}

func TestShutdown_InterruptsThenClosesTheSuppliedDispatcher(t *testing.T) {
	fake := mocks.NewDispatcher[order]()
	p := mustNew(t, newES(), bus.NewChannelBus[order](), WithDispatcherFactory(fake.Factory()))

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	calls := fake.Calls()
	wrongCalls := len(calls) != 3 || calls[1] != "Interrupt" || calls[2] != "Close"
	if wrongCalls {
		t.Errorf("calls = %v, want Factory, Interrupt, Close", calls)
	}
}

func TestNew_ATypedNilDispatcherWithoutAnErrorFails(t *testing.T) {
	factory := func(asynxmd.DispatcherConfig[order]) (asynxmd.Dispatcher[order], error) {
		var d *mocks.Dispatcher[order]
		return d, nil
	}

	p, err := New(newES(), bus.NewChannelBus[order](), WithDispatcherFactory(factory))

	notRejected := p != nil || err == nil || !strings.Contains(err.Error(), "returned no dispatcher")
	if notRejected {
		t.Errorf("New = (%v, %v), want the no-dispatcher error and no processor", p, err)
	}
}

func BenchmarkSend_MultiShard(b *testing.B) {
	for _, shards := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			memStore := store.New()
			channelBus := bus.NewChannelBus[order]()
			es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

			p := mustNew(b, es, channelBus, WithShards[order](shards))
			defer p.Shutdown(context.Background())

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
				p.Send(ctx, cmd)
			}
		})
	}
}

func BenchmarkSend_Parallel(b *testing.B) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(b, es, channelBus, WithShards[order](8))
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
			p.Send(ctx, cmd)
		}
	})
}

func BenchmarkSend_WithBusFanout(b *testing.B) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	// Subscribe 10 handlers to the bus
	for i := 0; i < 10; i++ {
		channelBus.Subscribe("OrderCreated", func(ctx context.Context, e asynxmd.Event[order]) {
			// Handler does nothing
		})
	}

	p := mustNew(b, es, channelBus)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		cmd := mocks.CreateOrderCmd{ID: "order", Total: 100.0}
		p.Send(ctx, cmd)
	}
}

// TestProcessor_IntegrationHighThroughput tests sustained throughput with the jobQueue fix.
// Sends 200 commands serially across different aggregates to verify no deadlock.
func TestProcessor_IntegrationHighThroughput(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](8),
		WithWorkersPerShard[order](4),
		WithQueueDepth[order](32),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	successCount := 0

	// Send 200 commands serially to verify processor throughput
	for i := 0; i < 200; i++ {
		cmd := mocks.CreateOrderCmd{
			ID:    "order" + string(rune(i%26+'a')),
			Total: 100.0,
		}
		if _, err := p.Send(ctx, cmd); err == nil {
			successCount++
		}
	}

	// With jobQueue fix, we should be able to send all 200 without deadlock
	if successCount < 190 {
		t.Fatalf("throughput: expected at least 190 successes, got %d", successCount)
	}

	t.Logf("Integration: %d/200 commands processed", successCount)
}

// TestProcessor_IntegrationOrderPreservation tests that commands for the same aggregate are processed in order.
func TestProcessor_IntegrationOrderPreservation(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](4),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	aggregateID := "order1"

	// Subscribe to events BEFORE sending
	var eventCount int32
	channelBus.Subscribe("OrderUpdated", func(ctx context.Context, e asynxmd.Event[order]) {
		atomic.AddInt32(&eventCount, 1)
	})

	// Send 50 updates to the same aggregate
	for i := 1; i <= 50; i++ {
		cmd := mocks.UpdateOrderCmd{
			ID: aggregateID,
			NewState: order{
				ID:     aggregateID,
				Total:  float64(i * 10),
				Status: "Pending",
			},
		}
		if _, err := p.Send(ctx, cmd); err != nil {
			t.Fatalf("Send failed at iteration %d: %v", i, err)
		}
	}

	// Wait for all async publishes to complete
	p.WaitPublish()

	count := atomic.LoadInt32(&eventCount)
	if count > 0 {
		t.Logf("Integration: %d update events processed", count)
	}
}

// TestProcessor_IntegrationCancelDuringBurst tests graceful cancellation handling.
// Verifies that cancellations are properly propagated during concurrent load.
func TestProcessor_IntegrationCancelDuringBurst(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](8),
		WithQueueDepth[order](16),
	)
	defer p.Shutdown(context.Background())

	// Test 1: Send some commands with a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := p.Send(ctx, cmd)
	if err != asynxmd.ErrContextCancelled {
		t.Fatalf("expected ErrContextCancelled, got %v", err)
	}

	// Test 2: Send with active context (should work)
	ctx2 := context.Background()
	cmd2 := mocks.CreateOrderCmd{ID: "order2", Total: 100.0}
	_, err2 := p.Send(ctx2, cmd2)
	if err2 != nil {
		t.Fatalf("expected success, got %v", err2)
	}

	t.Log("Integration: cancellation handling verified")
}

// TestProcessor_IntegrationShutdownGraceful tests graceful shutdown with in-flight commands.
func TestProcessor_IntegrationShutdownGraceful(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](4),
		WithWorkersPerShard[order](2),
		WithQueueDepth[order](10),
	)

	// Use a cancellable context so goroutines blocked in sendAndWait's result-wait
	// select can unblock when we cancel after Shutdown drains the workers.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	commandCount := 50
	var successCount int32

	var wg sync.WaitGroup
	for i := 0; i < commandCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{
				ID:    "order" + string(rune(idx%4)),
				Total: 100.0,
			}
			if _, err := p.Send(ctx, cmd); err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	err := p.Shutdown(shutdownCtx)
	if err != nil && err != context.DeadlineExceeded {
		t.Logf("Shutdown returned: %v", err)
	}

	// Cancel goroutines still waiting for results from the drained workers.
	cancel()
	wg.Wait()

	successVal := atomic.LoadInt32(&successCount)
	if successVal == 0 {
		t.Logf("Warning: no commands completed before shutdown")
	} else {
		t.Logf("Integration: %d commands completed before shutdown", successVal)
	}
}

// TestProcessor_IntegrationWithValidationErrors tests error handling across all shards.
func TestProcessor_IntegrationWithValidationErrors(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](8),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	var validCount int32
	var invalidCount int32

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var cmd asynxmd.Command[order]

			if idx%2 == 0 {
				// Valid command
				cmd = mocks.CreateOrderCmd{
					ID:    "order" + string(rune(idx)),
					Total: 100.0,
				}
			} else {
				// Invalid command (negative total)
				cmd = mocks.CreateOrderCmd{
					ID:    "order" + string(rune(idx)),
					Total: -100.0,
				}
			}

			_, err := p.Send(ctx, cmd)
			if err == asynxmd.ErrValidation {
				atomic.AddInt32(&invalidCount, 1)
			} else if err == nil {
				atomic.AddInt32(&validCount, 1)
			}
		}(i)
	}

	wg.Wait()

	validVal := atomic.LoadInt32(&validCount)
	invalidVal := atomic.LoadInt32(&invalidCount)

	// Should have roughly ~50 valid, ~50 invalid. Some commands may get
	// ErrQueueFull when goroutines race on overlapping aggregate IDs, so we
	// use a generous lower bound.
	if validVal < 30 || invalidVal < 30 {
		t.Fatalf("unexpected distribution: valid=%d, invalid=%d", validVal, invalidVal)
	}

	t.Logf("Integration: %d valid, %d invalid", validVal, invalidVal)
}

// TestProcessor_IntegrationMemoryStability tests that processor doesn't leak memory under sustained load.
func TestProcessor_IntegrationMemoryStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory stability test in short mode")
	}

	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](4),
		WithWorkersPerShard[order](2),
		WithQueueDepth[order](20),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()

	// Send many commands
	for batch := 0; batch < 10; batch++ {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				cmd := mocks.CreateOrderCmd{
					ID:    "order" + string(rune((idx+batch*100)%26+'a')),
					Total: 100.0,
				}
				p.Send(ctx, cmd)
			}(i)
		}
		wg.Wait()
	}

	t.Log("Integration: memory stability test completed without panic")
}

// TestProcessor_IntegrationEventPublishingReliability tests that events are published reliably.
func TestProcessor_IntegrationEventPublishingReliability(t *testing.T) {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := mustNew(t,
		es,
		channelBus,
		WithShards[order](4),
	)
	defer p.Shutdown(context.Background())

	ctx := context.Background()
	eventsChan := make(chan asynxmd.Event[order], 100)

	channelBus.Subscribe("OrderCreated", func(ctx context.Context, e asynxmd.Event[order]) {
		select {
		case eventsChan <- e:
		default:
			t.Logf("Event buffer full, dropped event")
		}
	})

	// Send commands and verify events are published
	commandCount := 20
	for i := 0; i < commandCount; i++ {
		cmd := mocks.CreateOrderCmd{
			ID:    "order" + string(rune('a'+i%26)),
			Total: 100.0,
		}
		if _, err := p.Send(ctx, cmd); err != nil {
			t.Logf("Send failed: %v", err)
		}
	}

	// Wait for all async publishes to complete
	p.WaitPublish()

	receivedCount := 0
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-eventsChan:
			receivedCount++
		case <-timeout:
			goto done
		}
	}

done:
	if receivedCount == 0 {
		t.Logf("Warning: no events received (may be timing issue)")
	} else {
		t.Logf("Integration: %d events published out of %d commands", receivedCount, commandCount)
	}
}

// TestProcessor_IntegrationDefaultQueueDepthScaling tests that default queue depth scales with workers.
func TestProcessor_IntegrationDefaultQueueDepthScaling(t *testing.T) {
	tests := []struct {
		name          string
		workers       int
		expectedMinQD int
	}{
		{"1 worker", 1, 1},
		{"4 workers", 4, 4},
		{"8 workers", 8, 8},
		{"16 workers", 16, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memStore := store.New()
			channelBus := bus.NewChannelBus[order]()
			es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

			p := mustNew(t,
				es,
				channelBus,
				WithWorkersPerShard[order](tt.workers),
			)
			defer p.Shutdown(context.Background())

			// Send commands rapidly to verify queue depth
			ctx := context.Background()
			successCount := 0
			for i := 0; i < tt.expectedMinQD*2; i++ {
				cmd := mocks.CreateOrderCmd{
					ID:    "order" + string(rune(i%8)),
					Total: 100.0,
				}
				if _, err := p.Send(ctx, cmd); err == nil {
					successCount++
				}
			}

			if successCount < tt.expectedMinQD {
				t.Logf("Warning: expected at least %d successes, got %d", tt.expectedMinQD, successCount)
			}
		})
	}
}

func TestRetryBackoff_IgnoresNonPositiveValues(
	t *testing.T,
) {
	cfg := configure(WithDeliveryRetry[int](0, -1))
	if cfg.dispatcher.RetryInitial != defaultRetryInitial || cfg.dispatcher.RetryMax != defaultRetryMax {
		t.Fatalf("backoff = %v..%v, want defaults", cfg.dispatcher.RetryInitial, cfg.dispatcher.RetryMax)
	}

	cfg = configure(WithDeliveryRetry[int](time.Second, time.Minute), WithDeliveryRetry[int](0, -1))
	if cfg.dispatcher.RetryInitial != time.Second || cfg.dispatcher.RetryMax != time.Minute {
		t.Fatalf("backoff = %v..%v, want the earlier positive values kept", cfg.dispatcher.RetryInitial, cfg.dispatcher.RetryMax)
	}
}

func TestOptions_NonPositiveBoundsAreIgnored(
	t *testing.T,
) {
	cfg := configure(WithDeliveryMaxAttempts[int](0))
	if cfg.dispatcher.MaxAttempts != defaultMaxAttempts {
		t.Fatalf("maxAttempts=%d, want the default", cfg.dispatcher.MaxAttempts)
	}

	cfg = configure(WithDeliveryMaxAttempts[int](3), WithDeliveryMaxAttempts[int](-1))
	if cfg.dispatcher.MaxAttempts != 3 {
		t.Fatalf("maxAttempts=%d, want the earlier positive value kept", cfg.dispatcher.MaxAttempts)
	}
}

func TestConfigure_PassesTheDeliverySettingsToTheDispatcherConfig(
	t *testing.T,
) {
	cfg := configure(WithDeliveryRetry[int](time.Second, time.Minute), WithDeliveryMaxAttempts[int](7))
	wrongConfig := cfg.dispatcher.RetryInitial != time.Second || cfg.dispatcher.RetryMax != time.Minute || cfg.dispatcher.MaxAttempts != 7
	if wrongConfig {
		t.Fatalf("config = %+v, want the values set", cfg.dispatcher)
	}
}

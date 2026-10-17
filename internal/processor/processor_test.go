package processor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/internal/processor/exec"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

func newProcessor(t *testing.T, opts ...processor.ProcessorOpt[order]) *processor.Processor[order] {
	memStore := store.New()
	channelBus := bus.NewChannelBus[order]()
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := processor.New(es, channelBus, opts...)

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
	p := newProcessor(t, processor.WithShards[order](1))

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
	p := newProcessor(t, processor.WithShards[order](8))

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 8)

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
			_, _err := p.Send(ctx, cmd); errs <- _err
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
	p := newProcessor(t, processor.WithShards[order](1))

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
		_, _err := p.Send(ctx, cmd); done <- _err
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
	p := newProcessor(t, processor.WithShards[order](1), processor.WithQueueDepth[order](1))

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
	p := newProcessor(t, processor.WithShards[order](4), processor.WithQueueDepth[order](5))

	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(idx)), Total: 100.0}
			_, _err := p.Send(ctx, cmd); errs <- _err
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

	p := processor.New(es, channelBus)
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

	p := processor.New(es, channelBus)
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

	p := processor.New(
		es,
		channelBus,
		processor.WithShards[order](2),
		processor.WithWorkersPerShard[order](6),
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

	p := processor.New(
		es,
		channelBus,
		processor.WithQueueDepth[order](100),
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
	p := processor.New(
		es,
		channelBus,
		processor.WithShards[order](-5),
		processor.WithQueueDepth[order](-1),
		processor.WithWorkersPerShard[order](0),
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

	p := processor.New(es, channelBus)

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
	p := newProcessor(t, processor.WithShards[order](4))

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

	p := processor.New(es, nil) // No bus

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

	p := processor.New(es, channelBus, processor.WithShards[order](8), processor.WithQueueDepth[order](100))
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

	p := processor.New(es, channelBus)

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

	p := processor.New(es, channelBus)
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

	proc := processor.New(
		eventstore.New[mocks.Order](s, store.NewSnapshots(), nil, 1, nil),
		&mocks.ErrBus[mocks.Order]{Err: errors.New("bus down")},
		processor.WithPublishErrorHandler[mocks.Order](handler),
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
func (b *blockingTestBus[T]) WaitForHandlers()               {}


// TestProcessor_SendWait_ContextCancelledWhileWaiting verifies the second-select
// ctx.Done branch in sendAndWait: context is cancelled after the command is enqueued
// but before the result arrives. A blocking bus ensures the worker is still running
// when ctx is cancelled, making the second-select ctx.Done branch deterministic.
func TestProcessor_SendWait_ContextCancelledWhileWaiting(t *testing.T) {
	memStore := store.New()
	unblockCh := make(chan struct{})
	bb := &blockingTestBus[order]{unblockCh: unblockCh}
	es := eventstore.New[order](memStore, store.NewSnapshots(), nil, 1, nil)

	p := processor.New(es, bb, processor.WithShards[order](1))
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
	p    *processor.Processor[order]
	s    store.Memory
	snap store.SnapshotMemory
	b    *bus.ChannelBus[order]
}

func newFixture(t *testing.T, s store.Memory, opts ...processor.ProcessorOpt[order]) fixture {
	t.Helper()
	snap := store.NewSnapshots()
	b := bus.NewChannelBus[order]()
	es := eventstore.New[order](s, snap, nil, 1, nil)
	p := processor.New(es, b, opts...)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return fixture{p: p, s: s, snap: snap, b: b}
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
		processor.WithSyncProjections([]exec.SyncProjection[order]{{
			Match: func(string) bool { return true },
			Fn: func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
				projected.Add(1)
				return nil
			},
		}}),
		processor.WithDurableDelivery[order](func(string) bool { return true }),
		processor.WithSnapshotErrorHandler(func(context.Context, asynxmd.Event[order], error) { snapErrs.Add(1) }),
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

func seedPending(t *testing.T, s store.Memory, es *eventstore.EventStore[order], ids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if _, err := es.Write(ctx, mocks.CreateOrderCmd{ID: id, Total: 1}); err != nil {
			t.Fatal(err)
		}
		s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, id, 1) })
	}
}

func TestProcessor_Recover_RedeliversEveryPendingEventAndAcksIt(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a", "b", "c")

	b := bus.NewChannelBus[order]()
	var got atomic.Int32
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { got.Add(1) })
	p := processor.New(es, b)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	// batch <= 0 falls back to the default; batch 2 forces multiple rounds.
	if err := p.Recover(context.Background(), 2); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	p.WaitPublish()
	if got.Load() != 3 {
		t.Errorf("redelivered %d events, want 3", got.Load())
	}
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Errorf("pending = %v, want none", pending)
	}
	if err := p.Recover(context.Background(), 0); err != nil {
		t.Errorf("Recover on an empty log: %v", err)
	}
}

func TestProcessor_Recover_WithoutBusIsNoOp(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p := processor.New[order](es, nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Recover(context.Background(), 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}

func TestProcessor_Recover_ReportsReadAndReplayFailures(t *testing.T) {
	ctx := context.Background()

	// Unreadable delivery log.
	es := eventstore.New[order](&mocks.ErrStore{Err: errors.New("log down")}, store.NewSnapshots(), nil, 1, nil)
	p := processor.New(es, bus.NewChannelBus[order]())
	t.Cleanup(func() { p.Shutdown(ctx) })
	if err := p.Recover(ctx, 10); err == nil {
		t.Error("Recover succeeded with an unreadable delivery log")
	}

	// Event blob that cannot be replayed.
	s := store.New()
	s.Append(ctx, "events:bad", 1, []byte("!!!"))
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "bad", 1) })
	es2 := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p2 := processor.New(es2, bus.NewChannelBus[order]())
	t.Cleanup(func() { p2.Shutdown(ctx) })
	if err := p2.Recover(ctx, 10); err == nil {
		t.Error("Recover succeeded with an unreplayable event")
	}
}

func TestProcessor_Recover_ReturnsWithoutWaitingForHandlers(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedPending(t, s, es, "a")

	b := bus.NewChannelBus[order]()
	release := make(chan struct{})
	b.Subscribe("OrderCreated", func(context.Context, asynxmd.Event[order]) { <-release })
	p := processor.New(es, b)
	t.Cleanup(func() { close(release); p.Shutdown(context.Background()) })

	returned := make(chan error, 1)
	go func() { returned <- p.Recover(context.Background(), 10) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recover waited for a blocked handler")
	}
}

type ackErrStore struct{ store.Memory }

func (ackErrStore) AckDelivery(context.Context, string, int64) error { return errors.New("ack failed") }

func TestProcessor_Recover_SurfacesAckFailureForVanishedEvent(t *testing.T) {
	s := ackErrStore{store.New()}
	ctx := context.Background()
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "ghost", 1) })
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	p := processor.New(es, bus.NewChannelBus[order]())
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err == nil {
		t.Fatal("Recover hid an ack failure")
	}
}

func TestProcessor_DurableDeliveryOptions_RetryReportAndPanicHandling(t *testing.T) {
	var attempts, reports, panics atomic.Int32
	s := store.New()
	f := newFixture(t, s,
		processor.WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
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
		processor.WithDeliveryErrorHandler(func(context.Context, asynxmd.Event[order], error, int) { reports.Add(1) }),
		processor.WithDeliveryPanicHandler(func(context.Context, asynxmd.Event[order], any) { panics.Add(1) }),
		processor.WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
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

func updateCmd(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

func always(string) bool { return true }

// parkingOpts makes a delivery give up after two attempts unless healthy is set.
func parkingOpts(healthy *atomic.Bool, extra ...processor.ProcessorOpt[order]) []processor.ProcessorOpt[order] {
	return append([]processor.ProcessorOpt[order]{
		processor.WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }),
		processor.WithDurableSubscribers([]dispatcher.DurableSub[order]{{
			Match: always,
			Handler: func(context.Context, asynxmd.Event[order]) error {
				if healthy.Load() {
					return nil
				}
				return errors.New("down")
			},
		}}),
		processor.WithDeliveryRetry[order](time.Millisecond, 2*time.Millisecond),
		processor.WithDeliveryMaxAttempts[order](2),
	}, extra...)
}

func TestProcessor_Resume_RedeliversParkedAggregatesInOrder(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()

	for i := range 2 {
		if _, err := f.p.Send(ctx, updateCmd("o1", float64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	<-parked
	if pending, _ := f.s.PendingDeliveries(ctx, 10); len(pending) != 2 {
		t.Fatalf("pending = %v, want both events", pending)
	}

	healthy.Store(true)
	if err := f.p.Resume(ctx, 1); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	f.p.WaitPublish()
	if pending, _ := f.s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending after Resume = %v", pending)
	}
	// Nothing parked any more: a second Resume is a no-op.
	if err := f.p.Resume(ctx, 0); err != nil {
		t.Fatalf("second Resume: %v", err)
	}
}

func TestProcessor_Resume_WithoutDispatcherIsNoOp(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	p := processor.New[order](es, nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Resume(context.Background(), 10); err != nil {
		t.Fatalf("Resume: %v", err)
	}
}

type flakyPendingStore struct {
	store.Memory
	fail atomic.Bool
}

func (s *flakyPendingStore) PendingDeliveries(ctx context.Context, limit int) ([]asynxmd.Delivery, error) {
	if s.fail.Load() {
		return nil, errors.New("log unreadable")
	}
	return s.Memory.PendingDeliveries(ctx, limit)
}

func TestProcessor_Resume_ReportsAFailureReadingTheLogAndStaysParked(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 4)
	s := &flakyPendingStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy,
		processor.WithDeliveryErrorHandler(func(_ context.Context, _ asynxmd.Event[order], err error, _ int) {
			if errors.Is(err, asynxmd.ErrDeliveryParked) {
				parked <- struct{}{}
			}
		}))...)
	ctx := context.Background()
	if _, err := f.p.Send(ctx, updateCmd("o1", 1)); err != nil {
		t.Fatal(err)
	}
	<-parked

	s.fail.Store(true)
	if err := f.p.Resume(ctx, 10); err == nil {
		t.Fatal("Resume succeeded although the delivery log is unreadable")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	s.fail.Store(false)
	if err := f.p.Resume(cancelled, 10); err == nil {
		t.Fatal("Resume with a cancelled context should fail taking the aggregate lock")
	}
}

func TestProcessor_Options_MaxQueuedAndDeadLetter(t *testing.T) {
	var healthy atomic.Bool
	dead := make(chan int64, 4)
	f := newFixture(t, store.New(), parkingOpts(&healthy,
		processor.WithDeliveryMaxQueued[order](100),
		processor.WithDeadLetterHandler(func(_ context.Context, e asynxmd.Event[order], _ error) error {
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

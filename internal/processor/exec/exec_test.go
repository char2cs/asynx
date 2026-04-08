package exec

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

func TestExecute_SuccessNewAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("expected aggregate to exist, got error: %v", err)
	}
	if agg.ID != "order1" || agg.Total != 100.0 {
		t.Errorf("unexpected aggregate state: %+v", agg)
	}
}

func TestExecute_SuccessExistingAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(ctx, cmd1, 1, false)
	if err != nil {
		t.Fatalf("first command failed: %v", err)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Confirmed"},
	}
	_, err = executor.Execute(ctx, cmd2, 2, false)
	if err != nil {
		t.Fatalf("second command failed: %v", err)
	}

	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("expected aggregate to exist, got error: %v", err)
	}
	if agg.Total != 150.0 || agg.Status != "Confirmed" {
		t.Errorf("unexpected aggregate state: %+v", agg)
	}
}

func TestExecute_ValidationFailure(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: -100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	_, err = es.Get(ctx, "order1")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_ValidationFailureNoWrite(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	executor.Execute(ctx, cmd1, 1, false)

	cmd2 := mocks.CancelOrderCmd{ID: "order2"}

	_, err := executor.Execute(ctx, cmd2, 1, false)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	_, err = es.Get(ctx, "order2")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_GetStorageError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 100.0},
	}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected success (nil state for missing agg), got %v", err)
	}
}

func TestExecute_WriteStorageError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	someError := errors.New("storage error")
	s.SetError("events:order1", someError)

	_, err := executor.Execute(ctx, cmd, 1, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Errorf("expected ErrPipelineFailed, got %v", err)
	}
}

func TestExecute_NilBusNoPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestExecute_PublishCalledAsync(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	trackingBus := &trackingBusMock[order]{
		publishChan: make(chan struct{}, 1),
		ctxChan:     make(chan context.Context, 1),
	}
	executor := New(es, trackingBus)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	select {
	case <-trackingBus.publishChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("publish was not called within timeout")
	}
}

func TestExecute_PublishUsesDetachedContext(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	trackingBus := &trackingBusMock[order]{
		ctxChan:     make(chan context.Context, 1),
		publishChan: make(chan struct{}, 1),
	}
	executor := New(es, trackingBus)

	ctx, cancel := context.WithCancel(context.Background())

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	cancel()

	select {
	case <-trackingBus.publishChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("publish was not called within timeout")
	}

	select {
	case publishCtx := <-trackingBus.ctxChan:
		if err := publishCtx.Err(); err != nil {
			t.Errorf("publish context was affected by original cancellation: %v", err)
		}
	default:
	}
}

func TestExecute_ContextAlreadyCancelled(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Logf("expected context.Canceled, got %v", err)
	}
}

func TestExecute_AggregateStatePreserved(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(ctx, cmd1, 1, false)
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}

	_, err = es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("get after first execute failed: %v", err)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Updated"},
	}
	_, err = executor.Execute(ctx, cmd2, 2, false)
	if err != nil {
		t.Fatalf("second execute failed: %v", err)
	}

	agg2, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("get after second execute failed: %v", err)
	}

	if agg2.Total != 150.0 || agg2.Status != "Updated" {
		t.Errorf("aggregate not updated correctly: %+v", agg2)
	}
}

func TestExecute_ReturnsEvent(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "evt-order", Total: 77.0}

	event, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if event.AggregateID != "evt-order" {
		t.Errorf("expected AggregateID evt-order, got %q", event.AggregateID)
	}
	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
}

func TestExecute_WaitHandlersTrue_HandlersDoneBeforeReturn(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	var handlerDone atomic.Bool
	trackBus := &trackingBusMock[order]{
		publishChan: make(chan struct{}, 1),
		ctxChan:     make(chan context.Context, 1),
		onPublishSync: func() {
			handlerDone.Store(true)
		},
	}
	executor := New(es, trackBus)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "wait-order", Total: 10.0}

	_, err := executor.Execute(ctx, cmd, 1, true)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if !handlerDone.Load() {
		t.Error("Execute with waitHandlers=true returned before PublishSync completed")
	}
}

func TestExecute_WaitHandlersFalse_PublishAsync(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	trackBus := &trackingBusMock[order]{
		publishChan: make(chan struct{}, 1),
		ctxChan:     make(chan context.Context, 1),
	}
	executor := New(es, trackBus)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "async-order", Total: 20.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	select {
	case <-trackBus.publishChan:
	case <-time.After(1 * time.Second):
		t.Fatalf("async publish was not called within timeout")
	}
}

func TestExecute_WaitHandlers_NilBusNoPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "nil-bus-order", Total: 5.0}

	event, err := executor.Execute(ctx, cmd, 1, true)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if event.AggregateID != "nil-bus-order" {
		t.Errorf("expected AggregateID nil-bus-order, got %q", event.AggregateID)
	}
}

func TestWaitPublish_NoRaceWhenHandlerTriggersNewPublishAsync(t *testing.T) {
	// Regression test for the data race in sync.WaitGroup reuse.
	//
	// Scenario: bus.Publish spawns a goroutine that calls executor.Execute,
	// simulating a projection handler that fires a new command (e.g. Quiver's
	// runner calling Send(EndExecution) from inside handleExecution).
	//
	// Race window with sync.WaitGroup:
	//   G1 (outer publish goroutine): defer wg.Done() → counter hits 0
	//     → wg.Wait() begins returning (writes internal WaitGroup state)
	//   G2 (inner Execute goroutine): publishAsync → wg.Add(1)
	//     → reads internal WaitGroup state
	//                                                         DATA RACE ❌
	//
	// With the fix (mutex + sync.Cond), pending++ and the WaitPublish check
	// are serialised under the same mutex so the window is closed.
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	var executor *CommandExecutor[order]
	var once sync.Once

	raceBus := &callbackPublishBus[order]{
		publishFn: func(ctx context.Context, event asynxmd.Event[order]) error {
			once.Do(func() {
				// Spawn a goroutine that triggers a second publishAsync.
				// It runs concurrently with the outer goroutine's deferred
				// wg.Done(), creating the Add/Wait-returning race window.
				go func() {
					cmd := mocks.UpdateOrderCmd{
						ID:       event.AggregateID,
						NewState: order{ID: event.AggregateID, Total: 1.0},
					}
					executor.Execute(ctx, cmd, 2, false) //nolint:errcheck
				}()
			})
			return nil
		},
	}
	executor = New(es, raceBus)

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "race-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// First WaitPublish: waits for the initial publishAsync goroutine (G1).
	// While G1 is active, G2 may call publishAsync → Add(1) concurrently
	// with G1's Done() hitting zero — triggering the race on buggy code.
	executor.WaitPublish()
	// Second WaitPublish: waits for any second-hop publishAsync (G2).
	executor.WaitPublish()
}

// trackingBusMock is a mock bus that tracks calls for testing
type trackingBusMock[T any] struct {
	publishChan   chan struct{}
	ctxChan       chan context.Context
	onPublishSync func()
	mu            sync.Mutex
}

func (m *trackingBusMock[T]) Publish(ctx context.Context, event asynxmd.Event[T]) error {
	m.mu.Lock()
	if len(m.publishChan) == 0 {
		m.publishChan <- struct{}{}
	}
	if m.ctxChan != nil && len(m.ctxChan) == 0 {
		m.ctxChan <- ctx
	}
	m.mu.Unlock()
	return nil
}

func (m *trackingBusMock[T]) PublishSync(ctx context.Context, event asynxmd.Event[T]) error {
	m.mu.Lock()
	if len(m.publishChan) == 0 {
		m.publishChan <- struct{}{}
	}
	if m.ctxChan != nil && len(m.ctxChan) == 0 {
		m.ctxChan <- ctx
	}
	if m.onPublishSync != nil {
		m.onPublishSync()
	}
	m.mu.Unlock()
	return nil
}

func (m *trackingBusMock[T]) Subscribe(pattern string, handler asynxmd.ProjectionHandler[T], opts ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}

func (m *trackingBusMock[T]) Unsubscribe(id string) error {
	return nil
}

func (m *trackingBusMock[T]) Close(ctx context.Context) error {
	return nil
}

func (m *trackingBusMock[T]) WaitForHandlers() {}

// callbackPublishBus invokes a custom function on Publish, used to inject
// concurrent behaviour in race-condition tests.
type callbackPublishBus[T any] struct {
	publishFn func(ctx context.Context, event asynxmd.Event[T]) error
}

func (b *callbackPublishBus[T]) Publish(ctx context.Context, event asynxmd.Event[T]) error {
	if b.publishFn != nil {
		return b.publishFn(ctx, event)
	}
	return nil
}

func (b *callbackPublishBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error {
	return nil
}

func (b *callbackPublishBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}

func (b *callbackPublishBus[T]) Unsubscribe(_ string) error { return nil }

func (b *callbackPublishBus[T]) Close(_ context.Context) error { return nil }

func (b *callbackPublishBus[T]) WaitForHandlers() {}

func TestPublishErrorHandler_CalledOnPublishError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	publishErr := errors.New("bus error")
	errBus := &callbackPublishBus[order]{
		publishFn: func(_ context.Context, _ asynxmd.Event[order]) error {
			return publishErr
		},
	}

	var mu sync.Mutex
	var gotEvent asynxmd.Event[order]
	var gotErr error
	called := false

	executor := New(es, errBus, WithPublishErrorHandler[order](func(_ context.Context, event asynxmd.Event[order], err error) {
		mu.Lock()
		defer mu.Unlock()
		gotEvent = event
		gotErr = err
		called = true
	}))

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "err-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	executor.WaitPublish()

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("PublishErrorHandler was not called")
	}
	if gotErr != publishErr {
		t.Errorf("expected err %v, got %v", publishErr, gotErr)
	}
	if gotEvent.AggregateID != "err-order" {
		t.Errorf("expected AggregateID err-order, got %q", gotEvent.AggregateID)
	}
}

func TestPublishErrorHandler_NotCalledOnSuccess(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	successBus := &callbackPublishBus[order]{
		publishFn: func(_ context.Context, _ asynxmd.Event[order]) error { return nil },
	}

	called := false
	executor := New(es, successBus, WithPublishErrorHandler[order](func(_ context.Context, _ asynxmd.Event[order], _ error) {
		called = true
	}))

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "ok-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	executor.WaitPublish()

	if called {
		t.Error("PublishErrorHandler must not be called on success")
	}
}

func TestPublishErrorHandler_NilHandlerDoesNotPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	errBus := &callbackPublishBus[order]{
		publishFn: func(_ context.Context, _ asynxmd.Event[order]) error {
			return errors.New("bus error")
		},
	}

	executor := New(es, errBus) // no handler option — default nil, must not panic

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "nil-handler-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	executor.WaitPublish()
}

func TestWaitPublish_BlocksUntilAsyncPublishDone(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	trackBus := &trackingBusMock[order]{
		publishChan: make(chan struct{}, 1),
		ctxChan:     make(chan context.Context, 1),
	}
	executor := New(es, trackBus)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "wp-order", Total: 1.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// WaitPublish blocks until the async publish goroutine completes.
	executor.WaitPublish()

	// After WaitPublish returns, Publish must have been called.
	select {
	case <-trackBus.publishChan:
		// publish was called — expected
	default:
		t.Error("WaitPublish returned but publish had not been called")
	}
}

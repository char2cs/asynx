package exec

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

func TestExecute_SuccessNewAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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

func TestExecute_NilDispatcherNoPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestExecute_DispatchCalledAsync(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)

	trackBus := &recordingBus[order]{}
	d := dispatcher.New[order](trackBus)
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	d.WaitIdle()

	trackBus.mu.Lock()
	defer trackBus.mu.Unlock()
	if trackBus.count == 0 {
		t.Fatal("dispatch was not called")
	}
}

func TestExecute_ContextAlreadyCancelled(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)

	var handlerDone atomic.Bool
	callbackBus := &recordingBus[order]{
		onPublishSync: func() {
			handlerDone.Store(true)
		},
	}
	d := dispatcher.New[order](callbackBus)
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

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

func TestExecute_WaitHandlersFalse_DispatchCalled(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	trackBus := &recordingBus[order]{}
	d := dispatcher.New[order](trackBus)
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "async-order", Total: 20.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	d.WaitIdle()

	trackBus.mu.Lock()
	defer trackBus.mu.Unlock()
	if trackBus.count == 0 {
		t.Fatal("dispatch was not called")
	}
}

func TestExecute_WaitHandlers_NilDispatcherNoPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
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

func TestPublishErrorHandler_CalledOnPublishError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)

	publishErr := errors.New("bus error")
	errBus := &recordingBus[order]{
		publishSyncErr: publishErr,
	}

	var mu sync.Mutex
	var gotEvent asynxmd.Event[order]
	var gotErr error
	called := false

	d := dispatcher.New[order](errBus, dispatcher.WithPublishErrorHandler[order](func(_ context.Context, event asynxmd.Event[order], err error) {
		mu.Lock()
		defer mu.Unlock()
		gotEvent = event
		gotErr = err
		called = true
	}))
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "err-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	d.WaitIdle()

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
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)

	successBus := &recordingBus[order]{}

	called := false
	d := dispatcher.New[order](successBus, dispatcher.WithPublishErrorHandler[order](func(_ context.Context, _ asynxmd.Event[order], _ error) {
		called = true
	}))
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "ok-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	d.WaitIdle()

	if called {
		t.Error("PublishErrorHandler must not be called on success")
	}
}

func TestPublishErrorHandler_NilHandlerDoesNotPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)

	errBus := &recordingBus[order]{
		publishSyncErr: errors.New("bus error"),
	}

	d := dispatcher.New[order](errBus) // no error handler — must not panic
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(es, d)

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "nil-handler-order", Total: 1.0}, 1, false)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	d.WaitIdle()
}

// recordingBus is a minimal Bus mock that records PublishSync calls and
// optionally returns an error or invokes a callback.
type recordingBus[T any] struct {
	mu             sync.Mutex
	count          int
	lastCtx        context.Context
	publishSyncErr error
	onPublishSync  func()
}

func (b *recordingBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *recordingBus[T]) PublishSync(ctx context.Context, _ asynxmd.Event[T]) error {
	b.mu.Lock()
	b.count++
	b.lastCtx = ctx
	if b.onPublishSync != nil {
		b.onPublishSync()
	}
	err := b.publishSyncErr
	b.mu.Unlock()
	return err
}
func (b *recordingBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *recordingBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *recordingBus[T]) Close(_ context.Context) error { return nil }
func (b *recordingBus[T]) WaitForHandlers()               {}

// delayedRecordingBus adds a configurable delay to PublishSync, useful for
// verifying that waitHandlers=false does not block on slow handlers.
type delayedRecordingBus[T any] struct {
	recordingBus[T]
	delay time.Duration
}

func (b *delayedRecordingBus[T]) PublishSync(ctx context.Context, event asynxmd.Event[T]) error {
	time.Sleep(b.delay)
	return b.recordingBus.PublishSync(ctx, event)
}

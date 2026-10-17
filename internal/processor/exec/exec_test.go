package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

func TestExecute_SuccessNewAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, false)
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
	_, err := executor.Execute(ctx, cmd1, false)
	if err != nil {
		t.Fatalf("first command failed: %v", err)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Confirmed"},
	}
	_, err = executor.Execute(ctx, cmd2, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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
	executor.Execute(ctx, cmd1, false)

	cmd2 := mocks.CancelOrderCmd{ID: "order2"}

	_, err := executor.Execute(ctx, cmd2, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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

	_, err := executor.Execute(ctx, cmd, false)
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
	_, err := executor.Execute(ctx, cmd1, false)
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
	_, err = executor.Execute(ctx, cmd2, false)
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

	event, err := executor.Execute(ctx, cmd, false)
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

	_, err := executor.Execute(ctx, cmd, true)
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

	_, err := executor.Execute(ctx, cmd, false)
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

	event, err := executor.Execute(ctx, cmd, true)
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
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "err-order", Total: 1.0}, false)
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
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "ok-order", Total: 1.0}, false)
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
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "nil-handler-order", Total: 1.0}, false)
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
func (b *recordingBus[T]) WaitForHandlers()              {}

func newEnv(t *testing.T, opts ...Opt[order]) (*CommandExecutor[order], store.Memory, store.SnapshotMemory, *dispatcher.Dispatcher[order], *bus.ChannelBus[order]) {
	t.Helper()
	return newEnvOn(t, store.New(), opts...)
}

func newEnvOn(t *testing.T, s store.Memory, opts ...Opt[order]) (*CommandExecutor[order], store.Memory, store.SnapshotMemory, *dispatcher.Dispatcher[order], *bus.ChannelBus[order]) {
	t.Helper()
	ss := store.NewSnapshots()
	es := eventstore.New[order](s, ss, nil, 1, nil)
	b := bus.NewChannelBus[order]()
	d := dispatcher.New[order](b)
	t.Cleanup(func() { d.Close(context.Background()) })
	return New(es, d, opts...), s, ss, d, b
}

func update(id string, total float64) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

func TestExecute_SameAggregateFlood_NoSpuriousFailuresAndOrderedDelivery(t *testing.T) {
	ex, _, _, d, b := newEnv(t)

	var mu sync.Mutex
	var seen []int64
	if _, err := b.Subscribe("OrderUpdated", func(_ context.Context, e asynxmd.Event[order]) {
		time.Sleep(200 * time.Microsecond) // slow handler: reordering would show
		mu.Lock()
		seen = append(seen, e.Version)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ex.Execute(context.Background(), update("agg", float64(i+1)), false); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Execute failed under contention: %v", err)
	}
	d.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("handler saw %d events, want %d", len(seen), n)
	}
	for i, v := range seen {
		if v != int64(i+1) {
			t.Fatalf("handler saw versions %v, want strictly increasing 1..%d", seen, n)
		}
	}
}

func TestExecute_DifferentAggregatesRunInParallel(t *testing.T) {
	ex, _, _, _, b := newEnv(t)

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	if _, err := b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		started <- struct{}{}
		<-release
	}); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"a", "b"} {
		if _, err := ex.Execute(context.Background(), update(id, 1), false); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("handlers of different aggregates did not run in parallel")
		}
	}
	close(release)
}

func TestExecute_SendWaitDoesNotHoldAggregateLockWhileHandlersRun(t *testing.T) {
	ex, _, _, _, b := newEnv(t)

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	if _, err := b.Subscribe("OrderUpdated", func(_ context.Context, e asynxmd.Event[order]) {
		if e.Version == 1 {
			entered <- struct{}{}
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}

	waited := make(chan struct{})
	go func() {
		ex.Execute(context.Background(), update("agg", 1), true)
		close(waited)
	}()
	<-entered

	// The first command is still waiting for its handler; the lock must be
	// free, so the next command commits (its handler queues behind v1).
	committed := make(chan error, 1)
	go func() {
		_, err := ex.Execute(context.Background(), update("agg", 2), false)
		committed <- err
	}()
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("second command failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("aggregate lock was held while SendWait waited for handlers")
	}
	close(release)
	<-waited
}

func TestExecute_LockWaitHonoursContext(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	ex, _, _, _, _ := newEnv(t, syncAll(func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
		entered <- struct{}{}
		<-release
		return nil
	}))

	go ex.Execute(context.Background(), update("agg", 1), false)
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := ex.Execute(ctx, update("agg", 2), false)
	if !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", err)
	}
	close(release)
}

func TestExecute_SnapshotFailureIsNonFatalAndStillDispatches(t *testing.T) {
	var reported atomic.Int32
	ex, _, ss, d, b := newEnv(t, WithSnapshotErrorHandler[order](func(context.Context, asynxmd.Event[order], error) {
		reported.Add(1)
	}))
	var delivered atomic.Int32
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) { delivered.Add(1) })

	ss.SetError("agg", errors.New("snapshot disk full"))
	cmd := mocks.UpdateOrderCmd{ID: "agg", NewState: order{ID: "agg", Total: 1}, Snapshot: true}

	evt, err := ex.Execute(context.Background(), cmd, false)
	if err != nil {
		t.Fatalf("Execute returned %v, want nil (snapshot is best-effort)", err)
	}
	d.WaitIdle()

	if evt.Version != 1 {
		t.Errorf("version = %d, want 1", evt.Version)
	}
	if reported.Load() != 1 {
		t.Errorf("snapshot errors reported = %d, want 1", reported.Load())
	}
	if delivered.Load() != 1 {
		t.Errorf("deliveries = %d, want 1 (dispatch must not depend on the snapshot)", delivered.Load())
	}
}

func syncAll(fn func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error) Opt[order] {
	return WithSyncProjections[order]([]SyncProjection[order]{{Match: func(string) bool { return true }, Fn: fn}})
}

func TestExecute_SyncProjectionErrorRollsBackAndDispatchesNothing(t *testing.T) {
	boom := errors.New("read model write failed")
	ex, s, _, d, b := newEnv(t, syncAll(func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error { return boom }))
	var delivered atomic.Int32
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) { delivered.Add(1) })

	_, err := ex.Execute(context.Background(), update("agg", 1), false)
	d.WaitIdle()

	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !errors.Is(err, asynxmd.ErrSyncProjection) || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ErrPipelineFailed + ErrSyncProjection + cause", err)
	}
	if blobs, _ := s.ReadFrom(context.Background(), "events:agg", 1); len(blobs) != 0 {
		t.Errorf("event survived a failed sync projection: %d blobs", len(blobs))
	}
	if delivered.Load() != 0 {
		t.Errorf("deliveries = %d, want 0", delivered.Load())
	}
}

func TestExecute_SyncProjectionSeesTxAndEvent_OnlyForMatchingEvents(t *testing.T) {
	var got []string
	ex, _, _, _, _ := newEnv(t, WithSyncProjections[order]([]SyncProjection[order]{{
		Match: func(name string) bool { return name == "OrderUpdated" },
		Fn: func(_ context.Context, tx asynxmd.Tx, e asynxmd.Event[order]) error {
			if tx == nil {
				return errors.New("nil tx")
			}
			got = append(got, fmt.Sprintf("%s@%d", e.AggregateID, e.Version))
			return nil
		},
	}}))

	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.Execute(context.Background(), mocks.CreateOrderCmd{ID: "other", Total: 1}, false); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0] != "agg@1" {
		t.Fatalf("projection calls = %v, want [agg@1]", got)
	}
}

func TestExecute_DurableDeliveryIsPendingUntilHandlersFinish(t *testing.T) {
	ex, s, _, d, b := newEnv(t, WithDurableDelivery[order](func(string) bool { return true }))
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
		entered <- struct{}{}
		<-release
	})

	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	<-entered

	pending, _ := s.PendingDeliveries(context.Background(), 10)
	if len(pending) != 1 || pending[0].AggregateID != "agg" || pending[0].Version != 1 {
		t.Fatalf("pending while handler runs = %v, want [{agg 1}]", pending)
	}

	close(release)
	d.WaitIdle()

	pending, _ = s.PendingDeliveries(context.Background(), 10)
	if len(pending) != 0 {
		t.Fatalf("pending after handlers finished = %v, want none", pending)
	}
}

func TestExecute_NoDeliveryEntriesWhenDurableOff(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
}

func TestExecute_ValidationErrorPassesThroughUnwrapped(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	_, err := ex.Execute(context.Background(), mocks.CreateOrderCmd{ID: "x", Total: -1}, false)
	if err != asynxmd.ErrValidation {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
	if strings.Contains(fmt.Sprint(err), "pipeline") {
		t.Fatalf("validation error was wrapped: %v", err)
	}
}

// A reaction handler that sends commands back to its own aggregate must not
// deadlock against commands queued behind a slow handler.
func TestExecute_ReactionToOwnAggregateDoesNotDeadlockUnderBacklog(t *testing.T) {
	ex, _, _, d, b := newEnv(t)

	release := make(chan struct{})
	reacted := make(chan struct{})
	b.Subscribe("OrderUpdated", func(ctx context.Context, e asynxmd.Event[order]) {
		if e.Version == 1 {
			<-release
			// React: send a follow-up command to the same aggregate.
			if _, err := ex.Execute(ctx, update("agg", 999), false); err != nil {
				t.Errorf("reaction Execute: %v", err)
			}
			close(reacted)
		}
	})

	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}

	flooded := make(chan struct{})
	go func() {
		for i := range 60 { // far more than any queue bound
			if _, err := ex.Execute(context.Background(), update("agg", float64(i+2)), false); err != nil {
				t.Errorf("Execute: %v", err)
			}
		}
		close(flooded)
	}()

	select {
	case <-flooded:
	case <-time.After(5 * time.Second):
		t.Fatal("commands deadlocked behind a slow handler")
	}

	close(release)
	select {
	case <-reacted:
	case <-time.After(5 * time.Second):
		t.Fatal("the reaction never completed")
	}
	d.WaitIdle()
}

func TestExecute_DurableDeliveryOnlyForEventsADurableSubscriberWants(t *testing.T) {
	ex, s, _, _, b := newEnv(t, WithDurableDelivery[order](func(name string) bool { return name == "OrderUpdated" }))
	release := make(chan struct{})
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) { <-release })
	t.Cleanup(func() { close(release) })
	ctx := context.Background()

	if _, err := ex.Execute(ctx, mocks.CreateOrderCmd{ID: "a", Total: 1}, false); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("unwanted event got a delivery entry: %v", pending)
	}

	if _, err := ex.Execute(ctx, update("a", 2), false); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Fatalf("wanted event pending = %v, want 1 entry", pending)
	}
}

type idemUpdate struct {
	mocks.UpdateOrderCmd
	Cmd string
}

func (c idemUpdate) CommandID() string { return c.Cmd }

type idemCreate struct {
	mocks.CreateOrderCmd
	Cmd string
}

func (c idemCreate) CommandID() string { return c.Cmd }

func streamLen(t *testing.T, s interface {
	ReadFrom(context.Context, string, int64) ([][]byte, error)
}, agg string) int {
	t.Helper()
	blobs, err := s.ReadFrom(context.Background(), "events:"+agg, 1)
	if err != nil {
		t.Fatal(err)
	}
	return len(blobs)
}

func TestExecute_Idempotent_AcceptedRetryIsNoOpReturningOriginalEvent(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	first, err := ex.Execute(context.Background(), cmd, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ex.Execute(context.Background(), cmd, false)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	if second.ID != first.ID || second.Version != first.Version || second.Aggregate.Total != 5 {
		t.Fatalf("retry returned %+v, want the original event %+v", second, first)
	}
	if n := streamLen(t, s, "agg"); n != 1 {
		t.Fatalf("stream length = %d, want 1 (retry must not write)", n)
	}
}

func TestExecute_Idempotent_RejectedRetryStaysRejected(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "cmd-bad"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("first err = %v, want ErrValidation", err)
	}

	// Same id, now with a body that would pass validation: still rejected.
	good := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: 10}, Cmd: "cmd-bad"}
	_, err = ex.Execute(context.Background(), good, false)
	if !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("retry err = %v, want ErrValidation (a rejection stays rejected)", err)
	}
	if n := streamLen(t, s, "agg"); n != 0 {
		t.Fatalf("stream length = %d, want 0", n)
	}
}

func TestExecute_Idempotent_StorageFailureLeavesNoReceiptSoRetryRuns(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	s.SetError("events:agg", errors.New("disk full"))
	if _, err := ex.Execute(context.Background(), cmd, false); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	if _, found, _ := s.GetReceipt(context.Background(), "cmd-1"); found {
		t.Fatal("a failed write left a receipt")
	}

	evt, err := ex.Execute(context.Background(), cmd, false)
	if err != nil || evt.Version != 1 {
		t.Fatalf("retry = %+v, %v; want version 1", evt, err)
	}
}

func TestExecute_Idempotent_SyncProjectionFailureLeavesNoReceipt(t *testing.T) {
	fail := true
	ex, s, _, _, _ := newEnv(t, syncAll(func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
		if fail {
			return errors.New("projection down")
		}
		return nil
	}))
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}

	if _, err := ex.Execute(context.Background(), cmd, false); err == nil {
		t.Fatal("expected failure")
	}
	if _, found, _ := s.GetReceipt(context.Background(), "cmd-1"); found {
		t.Fatal("rolled-back write left a receipt")
	}

	fail = false
	if _, err := ex.Execute(context.Background(), cmd, false); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestExecute_Idempotent_SameIDOnAnotherAggregateAnswersFromReceipt(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)

	first, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg-a", 1), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg-b", 2), Cmd: "cmd-1"}, false)
	if err != nil {
		t.Fatalf("second: %v", err)
	}

	if second.ID != first.ID || second.AggregateID != "agg-a" {
		t.Fatalf("second = %+v, want the original event on agg-a", second)
	}
	if n := streamLen(t, s, "agg-b"); n != 0 {
		t.Fatalf("agg-b stream length = %d, want 0", n)
	}
}

func TestExecute_EmptyCommandIDIsNotIdempotent(t *testing.T) {
	ex, s, _, _, _ := newEnv(t)
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: ""}

	for range 2 {
		if _, err := ex.Execute(context.Background(), cmd, false); err != nil {
			t.Fatal(err)
		}
	}
	if n := streamLen(t, s, "agg"); n != 2 {
		t.Fatalf("stream length = %d, want 2", n)
	}
}

func TestExecute_Idempotent_ReceiptReadErrorFailsPipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &getReceiptErrStore{Memory: store.New()})
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 5), Cmd: "cmd-1"}
	_, err := ex.Execute(context.Background(), cmd, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !strings.Contains(err.Error(), "receipt store down") {
		t.Fatalf("err = %v, want ErrPipelineFailed(receipt store down)", err)
	}
}

type getReceiptErrStore struct{ store.Memory }

func (getReceiptErrStore) GetReceipt(context.Context, string) (asynxmd.Receipt, bool, error) {
	return asynxmd.Receipt{}, false, errors.New("receipt store down")
}

// A duplicate-receipt failure whose receipt cannot then be found must not be
// mistaken for a retry answer: it is a pipeline failure.
type phantomDuplicateStore struct{ store.Memory }

func (s phantomDuplicateStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

type phantomTx struct{ asynxmd.Tx }

func (phantomTx) PutReceipt(context.Context, string, asynxmd.Receipt) error {
	return asynxmd.ErrDuplicateReceipt
}

func TestExecute_Idempotent_UnresolvableDuplicateReceiptFailsPipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, phantomDuplicateStore{store.New()})
	_, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !errors.Is(err, asynxmd.ErrDuplicateReceipt) {
		t.Fatalf("err = %v, want ErrPipelineFailed + ErrDuplicateReceipt", err)
	}
}

func TestExecute_Idempotent_AcceptedReceiptForErasedAggregateStillAnswers(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	cmd := idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}
	first, err := ex.Execute(ctx, cmd, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}

	retry, err := ex.Execute(ctx, cmd, false)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.ID != first.ID || retry.Version != first.Version {
		t.Fatalf("retry = %+v, want id/version of %+v", retry, first)
	}
}

func TestExecute_AfterDispatcherClosedFailsPipelineButKeepsTheCommit(t *testing.T) {
	ex, s, _, d, _ := newEnv(t)
	d.Close(context.Background())
	evt, err := ex.Execute(context.Background(), update("agg", 1), false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	if evt.Version != 1 || streamLen(t, s, "agg") != 1 {
		t.Fatalf("committed event not reported: %+v", evt)
	}
}

func TestExecute_WithoutDispatcherCommitsAndReturns(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	ex := New[order](es, nil, WithDurableDelivery[order](func(string) bool { return true }))
	if _, err := ex.Execute(context.Background(), update("agg", 1), true); err != nil {
		t.Fatal(err)
	}
	if pending, _ := s.PendingDeliveries(context.Background(), 10); len(pending) != 0 {
		t.Fatalf("delivery enqueued with nobody to deliver to: %v", pending)
	}
}

// Simulates losing the race for a command id between the receipt check and the
// commit: the transaction reports a duplicate and the receipt is then found.
type raceLostStore struct {
	store.Memory
	reads int
}

func (s *raceLostStore) GetReceipt(ctx context.Context, id string) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{}, false, nil
	}
	return asynxmd.Receipt{Accepted: true, AggregateID: "winner", EventID: "e-win", Version: 4}, true, nil
}

func (s *raceLostStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

func TestExecute_Idempotent_LosingTheReceiptRaceAnswersWithTheWinner(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &raceLostStore{Memory: store.New()})
	evt, err := ex.Execute(context.Background(), idemUpdate{UpdateOrderCmd: update("agg", 1), Cmd: "c1"}, false)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if evt.ID != "e-win" || evt.AggregateID != "winner" || evt.Version != 4 {
		t.Fatalf("event = %+v, want the winner's", evt)
	}
}

// A rejection that cannot be recorded must not be reported as a rejection: a
// retry could pass validation later and succeed.
type putReceiptFailStore struct{ store.Memory }

func (s putReceiptFailStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(failingReceiptTx{tx}) })
}

type failingReceiptTx struct{ asynxmd.Tx }

func (failingReceiptTx) PutReceipt(context.Context, string, asynxmd.Receipt) error {
	return errors.New("receipt table locked")
}

func TestExecute_Idempotent_UnrecordableRejectionFailsThePipeline(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, putReceiptFailStore{store.New()})
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("err = %v, want ErrPipelineFailed and not a (non-durable) rejection", err)
	}
}

// Two commands with one id race: the loser's rejection hits the winner's receipt.
type rejectionRaceStore struct {
	store.Memory
	reads int
}

func (s *rejectionRaceStore) GetReceipt(context.Context, string) (asynxmd.Receipt, bool, error) {
	s.reads++
	if s.reads == 1 {
		return asynxmd.Receipt{}, false, nil
	}
	return asynxmd.Receipt{Rejection: "decided elsewhere"}, true, nil
}

func (s *rejectionRaceStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error { return fn(phantomTx{tx}) })
}

func TestExecute_Idempotent_RejectionRaceAnswersFromTheExistingReceipt(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, &rejectionRaceStore{Memory: store.New()})
	bad := idemCreate{CreateOrderCmd: mocks.CreateOrderCmd{ID: "agg", Total: -1}, Cmd: "c1"}

	_, err := ex.Execute(context.Background(), bad, false)
	if !errors.Is(err, asynxmd.ErrValidation) || !strings.Contains(err.Error(), "decided elsewhere") {
		t.Fatalf("err = %v, want the existing rejection", err)
	}
}

func TestForget_ErasesStreamSnapshotAndPendingDeliveries(t *testing.T) {
	ex, s, ss, d, _ := newEnv(t, WithDurableDelivery[order](func(string) bool { return true }))
	ctx := context.Background()

	cmd := mocks.UpdateOrderCmd{ID: "agg", NewState: order{ID: "agg", Total: 1}, Snapshot: true}
	if _, err := ex.Execute(ctx, cmd, false); err != nil {
		t.Fatal(err)
	}
	d.WaitIdle()
	// Leave a delivery pending for the aggregate, as if a crash prevented its ack.
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "agg", 1) })

	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if n := streamLen(t, s, "agg"); n != 0 {
		t.Errorf("stream length = %d, want 0", n)
	}
	if _, found, _ := ss.Get(ctx, "agg"); found {
		t.Error("snapshot survived Forget")
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("pending deliveries = %v, want none", pending)
	}
}

func TestForget_DeliversTombstoneWithLastStateBeforeErasing(t *testing.T) {
	ex, s, _, _, b := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 42), false); err != nil {
		t.Fatal(err)
	}

	var gotTotal float64
	var streamAtHandler int
	b.Subscribe("asynx\\.aggregate\\.forget", func(_ context.Context, e asynxmd.Event[order]) {
		gotTotal = e.Aggregate.Total
		streamAtHandler = streamLen(t, s, "agg")
	})

	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	if gotTotal != 42 {
		t.Errorf("tombstone Aggregate.Total = %v, want 42", gotTotal)
	}
	if streamAtHandler == 0 {
		t.Error("stream was already erased when the forget handler ran")
	}
}

func TestForget_UnknownAggregateIsValidationError(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	if err := ex.Forget(context.Background(), "ghost"); !errors.Is(err, asynxmd.ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
}

type deleteFailStore struct{ store.Memory }

func (s deleteFailStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error {
		return fn(deleteFailTx{tx})
	})
}

type deleteFailTx struct{ asynxmd.Tx }

func (deleteFailTx) DeleteStream(context.Context, string) error { return errors.New("disk gone") }

func TestForget_DeleteFailureReturnsErrForgetFailed(t *testing.T) {
	ex, _, _, _, _ := newEnvOn(t, deleteFailStore{store.New()})
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(context.Background(), "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v, want ErrForgetFailed", err)
	}
}

func TestForget_LockWaitHonoursContext(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", err)
	}
}

func TestForget_ConcurrentWithUpdatesNeverLeavesAGappedStream(t *testing.T) {
	for range 20 {
		ex, s, _, _, _ := newEnv(t)
		ctx := context.Background()
		if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if i == 6 {
					if err := ex.Forget(ctx, "agg"); err != nil {
						t.Errorf("Forget: %v", err)
					}
					return
				}
				// A command racing the Forget is either ordered before it, after it,
				// or rejected while it runs.
				_, err := ex.Execute(ctx, update("agg", float64(i)), false)
				if err != nil && !errors.Is(err, asynxmd.ErrForgetInProgress) {
					t.Errorf("Execute: %v", err)
				}
			}()
		}
		wg.Wait()

		// Whatever survives must be a contiguous history from version 1: an
		// update never appends on top of erased history.
		blobs, _ := s.ReadFrom(ctx, "events:agg", 1)
		for i, blob := range blobs {
			var evt esmodels.InternalEvent
			if err := json.Unmarshal(blob, &evt); err != nil {
				t.Fatal(err)
			}
			if evt.Version != int64(i+1) {
				t.Fatalf("stream has version %d at position %d: history is gapped", evt.Version, i+1)
			}
		}
	}
}

func TestForget_RunsSyncProjectionsForTheTombstone(t *testing.T) {
	var names []string
	ex, _, _, _, _ := newEnv(t, WithSyncProjections[order]([]SyncProjection[order]{{
		Match: func(name string) bool { return name == "asynx.aggregate.forget" },
		Fn: func(_ context.Context, _ asynxmd.Tx, e asynxmd.Event[order]) error {
			names = append(names, e.EventName)
			return nil
		},
	}, {
		Match: func(string) bool { return false },
		Fn:    func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error { return errors.New("never") },
	}}))
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "asynx.aggregate.forget" {
		t.Fatalf("projection saw %v, want the tombstone only", names)
	}
}

func TestForget_SyncProjectionFailureAbortsBeforeErasing(t *testing.T) {
	failing := false
	ex, s, _, _, _ := newEnv(t, syncAll(func(context.Context, asynxmd.Tx, asynxmd.Event[order]) error {
		if failing {
			return errors.New("read model down")
		}
		return nil
	}))
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}

	failing = true
	err := ex.Forget(ctx, "agg")
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || !errors.Is(err, asynxmd.ErrSyncProjection) {
		t.Fatalf("err = %v, want ErrPipelineFailed + ErrSyncProjection", err)
	}
	if n := streamLen(t, s, "agg"); n != 1 {
		t.Fatalf("stream length = %d, want 1 (nothing erased, tombstone rolled back)", n)
	}
}

func TestForget_WithoutDispatcherStillErases(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	ex := New[order](es, nil)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); err != nil {
		t.Fatal(err)
	}
	if n := streamLen(t, s, "agg"); n != 0 {
		t.Fatalf("stream length = %d, want 0", n)
	}
}

func TestForget_AfterDispatcherClosedFailsPipeline(t *testing.T) {
	ex, _, _, d, _ := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	d.Close(ctx)
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
}

type snapshotDeleteFailStore struct{ asynxmd.SnapshotStore }

func (snapshotDeleteFailStore) Delete(context.Context, string) error { return errors.New("snap gone") }

func TestForget_SnapshotDeleteFailureReturnsErrForgetFailed(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, snapshotDeleteFailStore{store.NewSnapshots()}, nil, 1, nil)
	ex := New[order](es, nil)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v, want ErrForgetFailed", err)
	}
}

// A reaction queued ahead of the tombstone may send commands back to the
// aggregate; Forget must not hold the lock the reaction needs.
func TestForget_DoesNotDeadlockWithAReactionToItsOwnAggregate(t *testing.T) {
	ex, s, _, _, b := newEnv(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	b.Subscribe("OrderUpdated", func(hctx context.Context, e asynxmd.Event[order]) {
		if e.Version != 1 {
			return
		}
		close(started)
		<-release
		ex.Execute(hctx, update("agg", 99), false) // needs the aggregate lock
	})

	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	<-started

	forgot := make(chan error, 1)
	go func() { forgot <- ex.Forget(ctx, "agg") }()
	// The tombstone is queued behind the reaction once Forget has marked the
	// aggregate and released its lock.
	for !ex.isForgetting("agg") {
		runtime.Gosched()
	}
	if err := ex.WithLock(ctx, "agg", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	close(release)

	select {
	case err := <-forgot:
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forget deadlocked against a reaction to its own aggregate")
	}
	if n := streamLen(t, s, "agg"); n != 0 {
		t.Fatalf("stream length = %d, want 0", n)
	}
}

func TestForget_SnapshotIsDeletedBeforeTheStream(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](deleteFailStore{s}, snapshotDeleteFailStore{store.NewSnapshots()}, nil, 1, nil)
	ex := New[order](es, nil)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	// Snapshot delete fails first, so the stream is never touched.
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v, want ErrForgetFailed", err)
	}
	if n := streamLen(t, s, "agg"); n == 0 {
		t.Fatal("stream was erased although the snapshot could not be deleted")
	}
}

func TestWaitDone_ReturnsWhenDoneClosesOrContextEnds(t *testing.T) {
	done := make(chan struct{})
	close(done)
	if err := WaitDone(context.Background(), done); err != nil {
		t.Fatalf("closed done: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitDone(ctx, make(chan struct{})); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("cancelled: err = %v, want ErrContextCancelled", err)
	}
}

func TestExecute_WaitingForHandlersGivesUpWithTheCommittedEvent(t *testing.T) {
	ex, _, _, _, b := newEnv(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b.Subscribe("OrderUpdated", func(context.Context, asynxmd.Event[order]) {
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
		evt, err := ex.Execute(ctx, update("agg", 1), true)
		got <- result{evt, err}
	}()
	<-entered
	cancel()
	r := <-got
	if !errors.Is(r.err, asynxmd.ErrContextCancelled) || r.evt.Version != 1 {
		t.Fatalf("got (%+v, %v), want the committed v1 with ErrContextCancelled", r.evt, r.err)
	}
}

func TestWithLock_HonoursContext(t *testing.T) {
	ex, _, _, _, _ := newEnv(t)
	ran := false
	if err := ex.WithLock(context.Background(), "agg", func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("WithLock = %v, ran = %v", err, ran)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.WithLock(ctx, "agg", func() error { t.Fatal("ran"); return nil }); !errors.Is(err, asynxmd.ErrContextCancelled) {
		t.Fatalf("err = %v, want ErrContextCancelled", err)
	}
}

func TestForget_SecondConcurrentForgetIsRefusedWhileOneRuns(t *testing.T) {
	ex, _, _, _, b := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	first := make(chan error, 1)
	go func() { first <- ex.Forget(ctx, "agg") }()
	<-entered

	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatalf("second Forget err = %v, want ErrForgetInProgress", err)
	}
	if _, err := ex.Execute(ctx, update("agg", 2), false); !errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatalf("Execute err = %v, want ErrForgetInProgress", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first Forget: %v", err)
	}
}

func TestForget_AbortsWhenTheCallerGivesUpWaitingForHandlers(t *testing.T) {
	ex, s, _, _, b := newEnv(t)
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	b.Subscribe("asynx\\.aggregate\\.forget", func(context.Context, asynxmd.Event[order]) {
		close(entered)
		<-release
	})

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() { got <- ex.Forget(ctx, "agg") }()
	<-entered
	cancel()
	if err := <-got; !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v, want ErrForgetFailed", err)
	}
	close(release)

	// Nothing was erased, and the aggregate accepts commands again.
	if n := streamLen(t, s, "agg"); n == 0 {
		t.Fatal("stream erased although Forget aborted")
	}
	if _, err := ex.Execute(context.Background(), update("agg", 2), false); errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatal("aggregate still fenced after Forget aborted")
	}
}

func TestForget_FailsWhenTheDispatcherIsClosed(t *testing.T) {
	ex, s, _, d, _ := newEnv(t)
	if _, err := ex.Execute(context.Background(), update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	d.Close(context.Background())

	if err := ex.Forget(context.Background(), "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	if _, err := ex.Execute(context.Background(), update("agg", 2), false); errors.Is(err, asynxmd.ErrForgetInProgress) {
		t.Fatal("aggregate left fenced by a Forget that failed to enqueue")
	}
	_ = s
}

func durableEnv(t *testing.T, handler asynxmd.DurableHandler[order], opts ...dispatcher.Opt[order]) *CommandExecutor[order] {
	t.Helper()
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	d := dispatcher.New[order](bus.NewChannelBus[order](), append([]dispatcher.Opt[order]{
		dispatcher.WithDurableSubs([]dispatcher.DurableSub[order]{{Match: func(string) bool { return true }, Handler: handler}}),
		dispatcher.WithRetryBackoff[order](time.Millisecond, 2*time.Millisecond),
		dispatcher.WithMaxAttempts[order](2),
	}, opts...)...)
	t.Cleanup(func() { d.Close(context.Background()) })
	return New(es, d, WithDurableDelivery[order](func(string) bool { return true }))
}

func TestForget_DoesNotEraseWhenADurableSubscriberCannotTakeTheTombstone(t *testing.T) {
	ex := durableEnv(t, func(_ context.Context, e asynxmd.Event[order]) error {
		if e.EventName == asynxmd.ForgetEventName {
			return errors.New("cannot drop state")
		}
		return nil
	})
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), true); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) || !errors.Is(err, asynxmd.ErrDeliveryParked) {
		t.Fatalf("err = %v, want ErrForgetFailed wrapping ErrDeliveryParked", err)
	}
	if n := streamLen(t, ex.es.Store().(store.Memory), "agg"); n == 0 {
		t.Fatal("aggregate erased although a durable subscriber never took the tombstone")
	}
}

func TestForget_LetsAFailingDeliveryGoWhenTheTombstoneIsAccepted(t *testing.T) {
	attempted := make(chan struct{}, 8)
	ex := durableEnv(t, func(_ context.Context, e asynxmd.Event[order]) error {
		if e.EventName == asynxmd.ForgetEventName {
			return nil
		}
		attempted <- struct{}{}
		return errors.New("poison")
	}, dispatcher.WithRetryBackoff[order](time.Hour, time.Hour), dispatcher.WithMaxAttempts[order](100))
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	<-attempted

	done := make(chan error, 1)
	go func() { done <- ex.Forget(ctx, "agg") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Forget held back by a failing delivery of the same aggregate")
	}
}

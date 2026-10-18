package exec

import (
	"context"
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















func TestExecute_WaitingOnAParkedAggregateReportsNoHandlerRan(t *testing.T) {
	ex := durableEnv(t, func(context.Context, asynxmd.Event[order]) error { return nil })
	ex.dispatcher.Park("agg")

	evt, err := ex.Execute(context.Background(), update("agg", 1), true)
	if !errors.Is(err, asynxmd.ErrDeliveryParked) || evt.Version != 1 {
		t.Fatalf("Execute = v%d, %v; want the committed v1 with ErrDeliveryParked", evt.Version, err)
	}
	// Without waiting the command simply commits.
	if _, err := ex.Execute(context.Background(), update("agg", 2), false); err != nil {
		t.Fatalf("Execute without waiting: %v", err)
	}
}

// A command committed but not queued must not let the next one be delivered
// ahead of it.
func TestExecute_EnqueueFailureAfterCommitParksTheAggregate(t *testing.T) {
	ex, _, _, d, _ := newEnv(t, WithDurableDelivery[order](func(string) bool { return true }))
	d.Close(context.Background())

	evt, err := ex.Execute(context.Background(), update("agg", 1), false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) || evt.Version != 1 {
		t.Fatalf("Execute = v%d, %v; want the committed v1 with ErrPipelineFailed", evt.Version, err)
	}
	if got := d.Halted(); len(got) != 1 || got[0] != "agg" {
		t.Fatalf("halted = %v, want [agg]", got)
	}
}

func TestWithLockIfFree_RunsWhenFreeAndSkipsWhenHeld(t *testing.T) {
	es := eventstore.New[order](store.New(), store.NewSnapshots(), nil, 1, nil)
	executor := New(es, nil)

	ran := false
	if !executor.WithLockIfFree("agg", func() { ran = true }) || !ran {
		t.Fatal("fn did not run on a free aggregate")
	}

	var ranWhileHeld, reportedRunning bool
	err := executor.WithLock(context.Background(), "agg", func() error {
		reportedRunning = executor.WithLockIfFree("agg", func() { ranWhileHeld = true })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if ranWhileHeld || reportedRunning {
		t.Errorf("WithLockIfFree ran = %v, reported running = %v while the lock was held", ranWhileHeld, reportedRunning)
	}

	if !executor.WithLockIfFree("agg", func() {}) {
		t.Fatal("the lock was not released")
	}
}

func within[V any](
	t *testing.T,
	ch <-chan V,
	what string,
) V {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func eventually(
	t *testing.T,
	what string,
	cond func() bool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

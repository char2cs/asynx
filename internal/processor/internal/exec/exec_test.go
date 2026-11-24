package exec

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type order = mocks.Order

func TestExecute_SuccessNewAggregate(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	agg, err := es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"expected aggregate to exist, got error: %v",
			err,
		)
	}

	if agg.ID != "order1" ||
		agg.Total != 100.0 {
		t.Errorf(
			"unexpected aggregate state: %+v",
			agg,
		)
	}
}

func TestExecute_SuccessExistingAggregate(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(
		ctx,
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"first command failed: %v",
			err,
		)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Confirmed"},
	}
	_, err = executor.Execute(
		ctx,
		cmd2,
	)
	if err != nil {
		t.Fatalf(
			"second command failed: %v",
			err,
		)
	}

	agg, err := es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"expected aggregate to exist, got error: %v",
			err,
		)
	}

	if agg.Total != 150.0 ||
		agg.Status != "Confirmed" {
		t.Errorf(
			"unexpected aggregate state: %+v",
			agg,
		)
	}
}

func TestExecute_ValidationFailure(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: -100.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}

	_, err = es.Get(
		ctx,
		"order1",
	)
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_ValidationFailureNoWrite(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	executor.Execute(
		ctx,
		cmd1,
	)

	cmd2 := mocks.CancelOrderCmd{ID: "order2"}

	_, err := executor.Execute(
		ctx,
		cmd2,
	)
	if err != asynxmd.ErrValidation {
		t.Fatalf(
			"expected ErrValidation, got %v",
			err,
		)
	}

	_, err = es.Get(
		ctx,
		"order2",
	)
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_GetStorageError(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 100.0},
	}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected success (nil state for missing agg), got %v",
			err,
		)
	}
}

func TestExecute_WriteStorageError(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	someError := errors.New("storage error")
	s.SetError(
		"events:order1",
		someError,
	)

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Errorf(
			"expected ErrPipelineFailed, got %v",
			err,
		)
	}
}

func TestExecute_NilDispatcherNoPanic(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}
}

func TestExecute_DispatchCalledAsync(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	trackBus := &recordingBus[order]{}
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: trackBus})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	d.WaitIdle()

	trackBus.mu.Lock()
	defer trackBus.mu.Unlock()
	if trackBus.count == 0 {
		t.Fatal("dispatch was not called")
	}
}

func TestExecute_ContextAlreadyCancelled(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Logf(
			"expected context.Canceled, got %v",
			err,
		)
	}
}

func TestExecute_AggregateStatePreserved(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(
		ctx,
		cmd1,
	)
	if err != nil {
		t.Fatalf(
			"first execute failed: %v",
			err,
		)
	}

	_, err = es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"get after first execute failed: %v",
			err,
		)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Updated"},
	}
	_, err = executor.Execute(
		ctx,
		cmd2,
	)
	if err != nil {
		t.Fatalf(
			"second execute failed: %v",
			err,
		)
	}

	agg2, err := es.Get(
		ctx,
		"order1",
	)
	if err != nil {
		t.Fatalf(
			"get after second execute failed: %v",
			err,
		)
	}

	if agg2.Total != 150.0 ||
		agg2.Status != "Updated" {
		t.Errorf(
			"aggregate not updated correctly: %+v",
			agg2,
		)
	}
}

func TestExecute_ReturnsEvent(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "evt-order", Total: 77.0}

	event, err := executor.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"expected nil, got %v",
			err,
		)
	}

	if event.AggregateID != "evt-order" {
		t.Errorf(
			"expected AggregateID evt-order, got %q",
			event.AggregateID,
		)
	}

	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
}

func TestPublishErrorHandler_CalledOnPublishError(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	publishErr := errors.New("bus error")
	errBus := &recordingBus[order]{
		publishSyncErr: publishErr,
	}

	var mu sync.Mutex
	var gotEvent asynxmd.Event[order]
	var gotErr error
	called := false

	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: errBus, OnPublishError: func(
		_ context.Context,
		event asynxmd.Event[order],
		err error,
	) {
		mu.Lock()
		defer mu.Unlock()
		gotEvent = event
		gotErr = err
		called = true
	}})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	_, err := executor.Execute(
		ctx,
		mocks.CreateOrderCmd{ID: "err-order", Total: 1.0},
	)
	if err != nil {
		t.Fatalf(
			"Execute failed: %v",
			err,
		)
	}

	d.WaitIdle()

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("PublishErrorHandler was not called")
	}

	if gotErr != publishErr {
		t.Errorf(
			"expected err %v, got %v",
			publishErr,
			gotErr,
		)
	}

	if gotEvent.AggregateID != "err-order" {
		t.Errorf(
			"expected AggregateID err-order, got %q",
			gotEvent.AggregateID,
		)
	}
}

func TestPublishErrorHandler_NotCalledOnSuccess(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	successBus := &recordingBus[order]{}

	called := false
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: successBus, OnPublishError: func(
		_ context.Context,
		_ asynxmd.Event[order],
		_ error,
	) {
		called = true
	}})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	_, err := executor.Execute(
		ctx,
		mocks.CreateOrderCmd{ID: "ok-order", Total: 1.0},
	)
	if err != nil {
		t.Fatalf(
			"Execute failed: %v",
			err,
		)
	}

	d.WaitIdle()

	if called {
		t.Error("PublishErrorHandler must not be called on success")
	}
}

func TestPublishErrorHandler_NilHandlerDoesNotPanic(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	errBus := &recordingBus[order]{
		publishSyncErr: errors.New("bus error"),
	}

	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: errBus})
	t.Cleanup(func() { d.Close(context.Background()) })

	executor := New(
		es,
		d,
	)

	ctx := context.Background()
	_, err := executor.Execute(
		ctx,
		mocks.CreateOrderCmd{ID: "nil-handler-order", Total: 1.0},
	)
	if err != nil {
		t.Fatalf(
			"Execute failed: %v",
			err,
		)
	}

	d.WaitIdle()
}

type recordingBus[T any] struct {
	mu             sync.Mutex
	count          int
	lastCtx        context.Context
	publishSyncErr error
	onPublishSync  func()
}

// Publish accepts the event and does nothing.
func (b *recordingBus[T]) Publish(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	return nil
}

// PublishSync counts the call, records the context it was given in lastCtx,
// runs onPublishSync when set and returns publishSyncErr, so a test can
// assert what the dispatcher delivered and with which context.
func (b *recordingBus[T]) PublishSync(
	ctx context.Context,
	_ asynxmd.Event[T],
) error {
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

// Subscribe registers nothing and returns an empty id.
func (b *recordingBus[T]) Subscribe(
	_ string,
	_ asynxmd.ProjectionHandler[T],
	_ ...asynxmd.SubscriptionOpt[T],
) (string, error) {
	return "",
		nil
}

// Unsubscribe does nothing.
func (b *recordingBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close does nothing and succeeds.
func (b *recordingBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers returns at once; nothing is tracked.
func (b *recordingBus[T]) WaitForHandlers() {}

func newEnv(
	t *testing.T,
	opts ...Opt[order],
) (*CommandExecutor[order], store.Memory, store.SnapshotMemory, asynxmd.Dispatcher[order], asynxmd.Bus[order]) {
	t.Helper()
	return newEnvOn(
		t,
		store.New(),
		opts...,
	)
}

func newEnvOn(
	t *testing.T,
	s store.Memory,
	opts ...Opt[order],
) (*CommandExecutor[order], store.Memory, store.SnapshotMemory, asynxmd.Dispatcher[order], asynxmd.Bus[order]) {
	t.Helper()
	ss := store.NewSnapshots()
	es := eventstore.New[order](
		s,
		ss,
		nil,
		1,
		nil,
	)
	b := bus.New[order]()
	d := dispatcher.Dispatcher(asynxmd.DispatcherConfig[order]{Bus: b})
	t.Cleanup(func() { d.Close(context.Background()) })
	return New(
			es,
			d,
			opts...,
		),
		s,
		ss,
		d,
		b
}

func parked(
	d asynxmd.Dispatcher[order],
) []string {
	return slices.Sorted(maps.Keys(d.Parks()))
}

func update(
	id string,
	total float64,
) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: id, NewState: order{ID: id, Total: total}}
}

func TestExecute_SameAggregateFlood_NoSpuriousFailuresAndOrderedDelivery(
	t *testing.T,
) {
	ex, _, _, d, b := newEnv(t)

	recorder := &versionRecorder{}
	if _, err := b.Subscribe(
		"OrderUpdated",
		recorder.record,
	); err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(
		chan error,
		n,
	)
	for i := range n {
		wg.Add(1)
		go floodOne(
			ex,
			&wg,
			errs,
			float64(i+1),
		)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf(
			"Execute failed under contention: %v",
			err,
		)
	}

	d.WaitIdle()

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.seen) != n {
		t.Fatalf(
			"handler saw %d events, want %d",
			len(recorder.seen),
			n,
		)
	}

	for i, v := range recorder.seen {
		requireVersion(
			t,
			recorder.seen,
			i,
			v,
		)
	}
}

func floodOne(
	ex *CommandExecutor[order],
	wg *sync.WaitGroup,
	errs chan<- error,
	total float64,
) {
	defer wg.Done()
	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			total,
		),
	); err != nil {
		errs <- err
	}
}

func requireVersion(
	t *testing.T,
	seen []int64,
	i int,
	v int64,
) {
	t.Helper()
	if v != int64(i+1) {
		t.Fatalf(
			"handler saw versions %v, want strictly increasing 1..%d",
			seen,
			len(seen),
		)
	}
}

func TestExecute_DifferentAggregatesRunInParallel(
	t *testing.T,
) {
	ex, _, _, _, b := newEnv(t)

	gate := newReusableGate(2)
	if _, err := b.Subscribe(
		"OrderUpdated",
		gate.hold,
	); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"a", "b"} {
		mustExecute(
			t,
			ex,
			update(
				id,
				1,
			),
		)
	}

	for range 2 {
		awaitStarted(
			t,
			gate.entered,
		)
	}

	gate.open()
}

func mustExecute(
	t *testing.T,
	ex *CommandExecutor[order],
	cmd asynxmd.Command[order],
) {
	t.Helper()
	if _, err := ex.Execute(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatal(err)
	}
}

func awaitStarted(
	t *testing.T,
	started <-chan struct{},
) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handlers of different aggregates did not run in parallel")
	}
}

func awaitSecond(
	t *testing.T,
	committed <-chan error,
) {
	t.Helper()
	select {
	case err := <-committed:
		requireSecondCommitted(
			t,
			err,
		)
	case <-time.After(2 * time.Second):
		t.Fatal("aggregate lock was held while SendWait waited for handlers")
	}
}

func requireSecondCommitted(
	t *testing.T,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"second command failed: %v",
			err,
		)
	}
}

func TestExecute_SendWaitDoesNotHoldAggregateLockWhileHandlersRun(
	t *testing.T,
) {
	ex, _, _, _, b := newEnv(t)

	gate := newReusableGate(1)
	if _, err := b.Subscribe(
		"OrderUpdated",
		gate.holdFirstVersion,
	); err != nil {
		t.Fatal(err)
	}

	sender := &waitingSender{
		executor: ex,
		waited:   make(chan struct{}),
		committed: make(
			chan error,
			1,
		),
	}
	go sender.waitForHandlers()
	<-gate.entered

	go sender.commitSecond()
	awaitSecond(
		t,
		sender.committed,
	)

	gate.open()
	<-sender.waited
}

func TestExecute_LockWaitHonoursContext(
	t *testing.T,
) {
	gate := newReusableGate(1)
	ex, _, _, _, _ := newEnv(
		t,
		syncAll(gate.holdProjection),
	)

	go ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	<-gate.entered

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Millisecond,
	)
	defer cancel()
	_, err := ex.Execute(
		ctx,
		update(
			"agg",
			2,
		),
	)
	if !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"err = %v, want ErrContextCancelled",
			err,
		)
	}

	gate.open()
}

func TestExecute_SnapshotFailureIsNonFatalAndStillDispatches(
	t *testing.T,
) {
	reported := &eventCounter{}
	ex, _, ss, d, b := newEnv(
		t,
		WithSnapshotErrorHandler[order](reported.onSnapshotError),
	)
	delivered := &eventCounter{}
	b.Subscribe(
		"OrderUpdated",
		delivered.onEvent,
	)

	ss.SetError(
		"agg",
		errors.New("snapshot disk full"),
	)
	cmd := mocks.UpdateOrderCmd{ID: "agg", NewState: order{ID: "agg", Total: 1}, Snapshot: true}

	evt, err := ex.Execute(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"Execute returned %v, want nil (snapshot is best-effort)",
			err,
		)
	}

	d.WaitIdle()

	if evt.Version != 1 {
		t.Errorf(
			"version = %d, want 1",
			evt.Version,
		)
	}

	if reported.calls.Load() != 1 {
		t.Errorf(
			"snapshot errors reported = %d, want 1",
			reported.calls.Load(),
		)
	}

	if delivered.calls.Load() != 1 {
		t.Errorf(
			"deliveries = %d, want 1 (dispatch must not depend on the snapshot)",
			delivered.calls.Load(),
		)
	}
}

func matchAll(
	string,
) bool {
	return true
}

func matchUpdated(
	name string,
) bool {
	return name == "OrderUpdated"
}

func syncAll(
	fn func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error,
) Opt[order] {
	return WithSyncProjections[order]([]models.SyncProjection[order]{{Match: func(
		string,
	) bool {
		return true
	}, Fn: fn}})
}

func TestExecute_DurableDeliveryIsPendingUntilHandlersFinish(
	t *testing.T,
) {
	ex, s, _, d, b := newEnv(
		t,
		WithDurableDelivery[order](matchAll),
	)
	gate := newReusableGate(1)
	b.Subscribe(
		"OrderUpdated",
		gate.hold,
	)

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	<-gate.entered

	pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	)
	if len(pending) != 1 ||
		pending[0].AggregateID != "agg" ||
		pending[0].Version != 1 {
		t.Fatalf(
			"pending while handler runs = %v, want [{agg 1}]",
			pending,
		)
	}

	gate.open()
	d.WaitIdle()

	pending, _ = s.PendingDeliveries(
		context.Background(),
		10,
	)
	if len(pending) != 0 {
		t.Fatalf(
			"pending after handlers finished = %v, want none",
			pending,
		)
	}
}

func TestExecute_NoDeliveryEntriesWhenDurableOff(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none",
			pending,
		)
	}
}

func TestExecute_ValidationErrorPassesThroughUnwrapped(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	_, err := ex.Execute(
		context.Background(),
		mocks.CreateOrderCmd{ID: "x", Total: -1},
	)
	if err != asynxmd.ErrValidation {
		t.Fatalf(
			"err = %v, want ErrValidation",
			err,
		)
	}

	if strings.Contains(
		fmt.Sprint(err),
		"pipeline",
	) {
		t.Fatalf(
			"validation error was wrapped: %v",
			err,
		)
	}
}

func floodAggregate(
	t *testing.T,
	ex *CommandExecutor[order],
	flooded chan<- struct{},
) {
	for i := range 60 {
		executeReporting(
			t,
			ex,
			float64(i+2),
		)
	}

	close(flooded)
}

func executeReporting(
	t *testing.T,
	ex *CommandExecutor[order],
	total float64,
) {
	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			total,
		),
	); err != nil {
		t.Errorf(
			"Execute: %v",
			err,
		)
	}
}

func TestExecute_ReactionToOwnAggregateDoesNotDeadlockUnderBacklog(
	t *testing.T,
) {
	ex, _, _, d, b := newEnv(t)

	reaction := &ownAggregateReaction{
		t:       t,
		ex:      ex,
		release: make(chan struct{}),
		reacted: make(chan struct{}),
	}
	b.Subscribe(
		"OrderUpdated",
		reaction.react,
	)

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	flooded := make(chan struct{})
	go floodAggregate(
		t,
		ex,
		flooded,
	)

	select {
	case <-flooded:
	case <-time.After(5 * time.Second):
		t.Fatal("commands deadlocked behind a slow handler")
	}

	close(reaction.release)
	select {
	case <-reaction.reacted:
	case <-time.After(5 * time.Second):
		t.Fatal("the reaction never completed")
	}

	d.WaitIdle()
}

func TestExecute_DurableDeliveryOnlyForEventsADurableSubscriberWants(
	t *testing.T,
) {
	ex, s, _, _, b := newEnv(
		t,
		WithDurableDelivery[order](matchUpdated),
	)
	gate := newReusableGate(1)
	b.Subscribe(
		"OrderUpdated",
		gate.waitForRelease,
	)
	t.Cleanup(gate.open)
	ctx := context.Background()

	if _, err := ex.Execute(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"unwanted event got a delivery entry: %v",
			pending,
		)
	}

	if _, err := ex.Execute(
		ctx,
		update(
			"a",
			2,
		),
	); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"wanted event pending = %v, want 1 entry",
			pending,
		)
	}
}

func streamLen(
	t *testing.T,
	s interface {
		ReadFrom(
			context.Context,
			string,
			int64,
		) ([][]byte, error)
	},
	agg string,
) int {
	t.Helper()
	blobs, err := s.ReadFrom(
		context.Background(),
		"events:"+agg,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	return len(blobs)
}

func TestExecute_AfterDispatcherClosedFailsPipelineButKeepsTheCommit(
	t *testing.T,
) {
	ex, s, _, d, _ := newEnv(t)
	d.Close(context.Background())
	evt, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}

	if evt.Version != 1 ||
		streamLen(
			t,
			s,
			"agg",
		) != 1 {
		t.Fatalf(
			"committed event not reported: %+v",
			evt,
		)
	}
}

func TestExecute_WithoutDispatcherCommitsAndReturns(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ex := New[order](
		es,
		nil,
		WithDurableDelivery[order](matchAll),
	)
	if _, err := ex.ExecuteWait(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"delivery enqueued with nobody to deliver to: %v",
			pending,
		)
	}
}

func TestExecute_EnqueueFailureAfterCommitParksTheAggregate(
	t *testing.T,
) {
	ex, _, _, d, _ := newEnv(
		t,
		WithDurableDelivery[order](matchAll),
	)
	d.Close(context.Background())

	evt, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) ||
		evt.Version != 1 {
		t.Fatalf(
			"Execute = v%d, %v; want the committed v1 with ErrPipelineFailed",
			evt.Version,
			err,
		)
	}

	if got := parked(d); len(got) != 1 ||
		got[0] != "agg" {
		t.Fatalf(
			"halted = %v, want [agg]",
			got,
		)
	}
}

func TestWithLockIfFree_RunsWhenFreeAndSkipsWhenHeld(
	t *testing.T,
) {
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ran := false
	if !executor.WithLockIfFree(
		"agg",
		func() { ran = true },
	) ||
		!ran {
		t.Fatal("fn did not run on a free aggregate")
	}

	probe := &lockProbe{executor: executor}
	err := executor.WithLock(
		context.Background(),
		"agg",
		probe.tryWhileHeld,
	)
	if err != nil {
		t.Fatal(err)
	}

	if probe.ranWhileHeld ||
		probe.reportedRunning {
		t.Errorf(
			"WithLockIfFree ran = %v, reported running = %v while the lock was held",
			probe.ranWhileHeld,
			probe.reportedRunning,
		)
	}

	if !executor.WithLockIfFree(
		"agg",
		func() {},
	) {
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
		t.Fatalf(
			"timed out waiting for %s",
			what,
		)
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
		requireBefore(
			t,
			deadline,
			what,
		)
		runtime.Gosched()
	}
}

func requireBefore(
	t *testing.T,
	deadline time.Time,
	what string,
) {
	t.Helper()
	if time.Now().After(deadline) {
		t.Fatalf(
			"timed out waiting for %s",
			what,
		)
	}
}

func BenchmarkExecute_CreateNew(
	b *testing.B,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run(
			"events="+string(rune('0'+numEvents/100))+"_000",
			createBench(
				ctx,
				executor,
			),
		)
	}
}

func createBench(
	ctx context.Context,
	executor *CommandExecutor[order],
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		b.ReportAllocs()
		b.ResetTimer()
		runCreateLoop(
			ctx,
			b,
			executor,
		)
	}
}

func runCreateLoop(
	ctx context.Context,
	b *testing.B,
	executor *CommandExecutor[order],
) {
	for b.Loop() {
		cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(b.N)), Total: 100.0}
		_, _ = executor.Execute(
			ctx,
			cmd,
		)
	}
}

func BenchmarkExecute_UpdateExisting(
	b *testing.B,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	executor := New(
		es,
		nil,
	)

	ctx := context.Background()

	createCmd := mocks.CreateOrderCmd{ID: "order_base", Total: 100.0}
	executor.Execute(
		ctx,
		createCmd,
	)

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run(
			"events="+string(rune('0'+numEvents/100))+"_000",
			updateBench(
				ctx,
				executor,
			),
		)
	}
}

func updateBench(
	ctx context.Context,
	executor *CommandExecutor[order],
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		b.ReportAllocs()
		b.ResetTimer()
		runUpdateLoop(
			ctx,
			b,
			executor,
		)
	}
}

func runUpdateLoop(
	ctx context.Context,
	b *testing.B,
	executor *CommandExecutor[order],
) {
	for b.Loop() {
		cmd := mocks.UpdateOrderCmd{
			ID:       "order_base",
			NewState: order{ID: "order_base", Total: 150.0, Status: "Confirmed"},
		}
		_, _ = executor.Execute(
			ctx,
			cmd,
		)
	}
}

var errEnqueue = errors.New("enqueue refused")

func fakeEnv(
	t *testing.T,
) (*CommandExecutor[order], *mockbuses.Dispatcher[order]) {
	t.Helper()
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	fake := mockbuses.NewDispatcher[order]()
	return New[order](
			es,
			fake,
			WithDurableDelivery[order](matchAll),
		),
		fake
}

func TestExecute_EnqueueFailureParksWithTheEnqueueError(
	t *testing.T,
) {
	ex, fake := fakeEnv(t)
	fake.FailEnqueue(errEnqueue)

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); !errors.Is(
		err,
		errEnqueue,
	) {
		t.Fatalf(
			"Execute = %v, want the enqueue error",
			err,
		)
	}

	calls := fake.ParkCalls()
	wrongCalls := len(calls) != 1 || calls[0].AggregateID != "agg" || !errors.Is(
		calls[0].Cause,
		errEnqueue,
	)
	if wrongCalls {
		t.Fatalf(
			"ParkCalls = %+v, want agg parked with the enqueue error",
			calls,
		)
	}

	if got := fake.Inspect("agg"); !got.Parked ||
		!errors.Is(
			got.Cause,
			errEnqueue,
		) {
		t.Fatalf(
			"Inspect = %+v, want parked with the enqueue error",
			got,
		)
	}
}

type handlerGate struct {
	entered chan struct{}
	release chan struct{}
}

func newHandlerGate() *handlerGate {
	return &handlerGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *handlerGate) hold(
	context.Context,
	asynxmd.Event[order],
) {
	close(g.entered)
	<-g.release
}

type reusableGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newReusableGate(
	entries int,
) *reusableGate {
	return &reusableGate{
		entered: make(
			chan struct{},
			entries,
		),
		release: make(chan struct{}),
	}
}

func (g *reusableGate) hold(
	context.Context,
	asynxmd.Event[order],
) {
	g.entered <- struct{}{}
	<-g.release
}

func (g *reusableGate) holdFirstVersion(
	ctx context.Context,
	e asynxmd.Event[order],
) {
	if e.Version != 1 {
		return
	}

	g.hold(
		ctx,
		e,
	)
}

func (g *reusableGate) holdProjection(
	_ context.Context,
	_ asynxmd.Tx,
	_ asynxmd.Event[order],
) error {
	g.entered <- struct{}{}
	<-g.release
	return nil
}

func (g *reusableGate) waitForRelease(
	context.Context,
	asynxmd.Event[order],
) {
	<-g.release
}

func (g *reusableGate) open() {
	g.once.Do(g.close)
}

func (g *reusableGate) close() {
	close(g.release)
}

type eventCounter struct {
	calls atomic.Int32
}

func (c *eventCounter) onEvent(
	context.Context,
	asynxmd.Event[order],
) {
	c.calls.Add(1)
}

func (c *eventCounter) onSnapshotError(
	context.Context,
	asynxmd.Event[order],
	error,
) {
	c.calls.Add(1)
}

type versionRecorder struct {
	mu   sync.Mutex
	seen []int64
}

func (r *versionRecorder) record(
	_ context.Context,
	e asynxmd.Event[order],
) {
	time.Sleep(200 * time.Microsecond)
	r.mu.Lock()
	r.seen = append(
		r.seen,
		e.Version,
	)
	r.mu.Unlock()
}

type waitingSender struct {
	executor  *CommandExecutor[order]
	waited    chan struct{}
	committed chan error
}

func (s *waitingSender) waitForHandlers() {
	_, _ = s.executor.ExecuteWait(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	close(s.waited)
}

func (s *waitingSender) commitSecond() {
	_, err := s.executor.Execute(
		context.Background(),
		update(
			"agg",
			2,
		),
	)
	s.committed <- err
}

type ownAggregateReaction struct {
	t       *testing.T
	ex      *CommandExecutor[order]
	release chan struct{}
	reacted chan struct{}
}

func (r *ownAggregateReaction) react(
	ctx context.Context,
	e asynxmd.Event[order],
) {
	if e.Version != 1 {
		return
	}

	<-r.release
	if _, err := r.ex.Execute(
		ctx,
		update(
			"agg",
			999,
		),
	); err != nil {
		r.t.Errorf(
			"reaction Execute: %v",
			err,
		)
	}

	close(r.reacted)
}

type lockProbe struct {
	executor        *CommandExecutor[order]
	ranWhileHeld    bool
	reportedRunning bool
}

func (p *lockProbe) markRan() {
	p.ranWhileHeld = true
}

func (p *lockProbe) tryWhileHeld(
	context.Context,
) error {
	p.reportedRunning = p.executor.WithLockIfFree(
		"agg",
		p.markRan,
	)
	return nil
}

func enqueuingDelivery(
	ctx context.Context,
	aggregateID string,
	version int64,
) func(asynxmd.Tx) error {
	return func(
		tx asynxmd.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			aggregateID,
			version,
		)
	}
}

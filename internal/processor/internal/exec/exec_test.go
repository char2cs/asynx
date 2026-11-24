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

// Publish is the test double implementation for the code under test.
func (b *recordingBus[T]) Publish(
	_ context.Context,
	_ asynxmd.Event[T],
) error {
	return nil
}

// PublishSync is the test double implementation for the code under test.
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

// Subscribe is the test double implementation for the code under test.
func (b *recordingBus[T]) Subscribe(
	_ string,
	_ asynxmd.ProjectionHandler[T],
	_ ...asynxmd.SubscriptionOpt[T],
) (string, error) {
	return "",
		nil
}

// Unsubscribe is the test double implementation for the code under test.
func (b *recordingBus[T]) Unsubscribe(
	_ string,
) error {
	return nil
}

// Close is the test double implementation for the code under test.
func (b *recordingBus[T]) Close(
	_ context.Context,
) error {
	return nil
}

// WaitForHandlers is the test double implementation for the code under test.
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

	var mu sync.Mutex
	var seen []int64
	record := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		time.Sleep(200 * time.Microsecond)
		mu.Lock()
		seen = append(
			seen,
			e.Version,
		)
		mu.Unlock()
	}
	if _, err := b.Subscribe(
		"OrderUpdated",
		record,
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

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf(
			"handler saw %d events, want %d",
			len(seen),
			n,
		)
	}

	for i, v := range seen {
		requireVersion(
			t,
			seen,
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

	release := make(chan struct{})
	started := make(
		chan struct{},
		2,
	)
	signal := func(
		context.Context,
		asynxmd.Event[order],
	) {
		started <- struct{}{}
		<-release
	}
	if _, err := b.Subscribe(
		"OrderUpdated",
		signal,
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
			started,
		)
	}

	close(release)
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

func holdFirst(
	e asynxmd.Event[order],
	entered chan<- struct{},
	release <-chan struct{},
) {
	if e.Version != 1 {
		return
	}

	entered <- struct{}{}
	<-release
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

	release := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	hold := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		holdFirst(
			e,
			entered,
			release,
		)
	}
	if _, err := b.Subscribe(
		"OrderUpdated",
		hold,
	); err != nil {
		t.Fatal(err)
	}

	waited := make(chan struct{})
	waitFirst := func() {
		_, _ = ex.ExecuteWait(
			context.Background(),
			update(
				"agg",
				1,
			),
		)
		close(waited)
	}
	go waitFirst()
	<-entered

	committed := make(
		chan error,
		1,
	)
	commitSecond := func() {
		_, err := ex.Execute(
			context.Background(),
			update(
				"agg",
				2,
			),
		)
		committed <- err
	}
	go commitSecond()
	awaitSecond(
		t,
		committed,
	)

	close(release)
	<-waited
}

func TestExecute_LockWaitHonoursContext(
	t *testing.T,
) {
	release := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	blockProjection := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	ex, _, _, _, _ := newEnv(
		t,
		syncAll(blockProjection),
	)

	go ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	<-entered

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

	close(release)
}

func TestExecute_SnapshotFailureIsNonFatalAndStillDispatches(
	t *testing.T,
) {
	var reported atomic.Int32
	onSnapshotError := func(
		context.Context,
		asynxmd.Event[order],
		error,
	) {
		reported.Add(1)
	}
	ex, _, ss, d, b := newEnv(
		t,
		WithSnapshotErrorHandler[order](onSnapshotError),
	)
	var delivered atomic.Int32
	count := func(
		context.Context,
		asynxmd.Event[order],
	) {
		delivered.Add(1)
	}
	b.Subscribe(
		"OrderUpdated",
		count,
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

	if reported.Load() != 1 {
		t.Errorf(
			"snapshot errors reported = %d, want 1",
			reported.Load(),
		)
	}

	if delivered.Load() != 1 {
		t.Errorf(
			"deliveries = %d, want 1 (dispatch must not depend on the snapshot)",
			delivered.Load(),
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
	release := make(chan struct{})
	entered := make(
		chan struct{},
		1,
	)
	hold := func(
		context.Context,
		asynxmd.Event[order],
	) {
		entered <- struct{}{}
		<-release
	}
	b.Subscribe(
		"OrderUpdated",
		hold,
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

	<-entered

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

	close(release)
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

func reactOnce(
	ctx context.Context,
	t *testing.T,
	ex *CommandExecutor[order],
	e asynxmd.Event[order],
	release <-chan struct{},
	reacted chan<- struct{},
) {
	if e.Version != 1 {
		return
	}

	<-release
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			999,
		),
	); err != nil {
		t.Errorf(
			"reaction Execute: %v",
			err,
		)
	}

	close(reacted)
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

	release := make(chan struct{})
	reacted := make(chan struct{})
	react := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		reactOnce(
			ctx,
			t,
			ex,
			e,
			release,
			reacted,
		)
	}
	b.Subscribe(
		"OrderUpdated",
		react,
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

	close(release)
	select {
	case <-reacted:
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
	release := make(chan struct{})
	waitRelease := func(
		context.Context,
		asynxmd.Event[order],
	) {
		<-release
	}
	b.Subscribe(
		"OrderUpdated",
		waitRelease,
	)
	t.Cleanup(func() { close(release) })
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

	var ranWhileHeld, reportedRunning bool
	markRan := func() { ranWhileHeld = true }
	probe := func() error {
		reportedRunning = executor.WithLockIfFree(
			"agg",
			markRan,
		)
		return nil
	}
	err := executor.WithLock(
		context.Background(),
		"agg",
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}

	if ranWhileHeld ||
		reportedRunning {
		t.Errorf(
			"WithLockIfFree ran = %v, reported running = %v while the lock was held",
			ranWhileHeld,
			reportedRunning,
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

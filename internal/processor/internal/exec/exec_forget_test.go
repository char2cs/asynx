package exec

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func forgetAlways(
	string,
) bool {
	return true
}

func TestForget_ErasesStreamSnapshotAndPendingDeliveries(
	t *testing.T,
) {
	ex, s, ss, d, _ := newEnv(
		t,
		WithDurableDelivery[order](forgetAlways),
	)
	ctx := context.Background()

	cmd := mocks.UpdateOrderCmd{ID: "agg", NewState: order{ID: "agg", Total: 1}, Snapshot: true}
	if _, err := ex.Execute(
		ctx,
		cmd,
	); err != nil {
		t.Fatal(err)
	}

	d.WaitIdle()
	enqueue := func(
		tx asynxmd.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			"agg",
			1,
		)
	}
	if err := s.Atomically(
		ctx,
		enqueue,
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatalf(
			"Forget: %v",
			err,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 0 {
		t.Errorf(
			"stream length = %d, want 0",
			n,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg",
	); found {
		t.Error("snapshot survived Forget")
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Errorf(
			"pending deliveries = %v, want none",
			pending,
		)
	}
}

func TestForget_DeliversTombstoneWithLastStateBeforeErasing(
	t *testing.T,
) {
	ex, s, _, _, b := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			42,
		),
	); err != nil {
		t.Fatal(err)
	}

	var gotTotal float64
	var streamAtHandler int
	record := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		gotTotal = e.Aggregate.Total
		streamAtHandler = streamLen(
			t,
			s,
			"agg",
		)
	}
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		record,
	)

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	if gotTotal != 42 {
		t.Errorf(
			"tombstone Aggregate.Total = %v, want 42",
			gotTotal,
		)
	}

	if streamAtHandler == 0 {
		t.Error("stream was already erased when the forget handler ran")
	}
}

func TestForget_UnknownAggregateIsValidationError(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	if err := ex.Forget(
		context.Background(),
		"ghost",
	); !errors.Is(
		err,
		asynxmd.ErrValidation,
	) {
		t.Fatalf(
			"err = %v, want ErrValidation",
			err,
		)
	}
}

type deleteFailStore struct{ store.Memory }

// Atomically is the test double implementation for the code under test.
func (s deleteFailStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(deleteFailTx{tx})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

type deleteFailTx struct{ asynxmd.Tx }

// DeleteStream is the test double implementation for the code under test.
func (deleteFailTx) DeleteStream(
	context.Context,
	string,
) error {
	return errors.New("disk gone")
}

func TestForget_DeleteFailureReturnsErrForgetFailed(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		deleteFailStore{store.New()},
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

	if err := ex.Forget(
		context.Background(),
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}
}

func TestForget_ErasePhaseFailureParksTheAggregate(
	t *testing.T,
) {
	ex, _, _, d, _ := newEnvOn(
		t,
		deleteFailStore{store.New()},
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v",
			err,
		)
	}

	if got := parked(d); !slices.Equal(
		got,
		[]string{"agg"},
	) {
		t.Fatalf(
			"halted = %v, want [agg]",
			got,
		)
	}

	if ex.isForgetting("agg") {
		t.Fatal("fence left up after the failed erase")
	}
}

func TestForget_LockWaitHonoursContext(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"err = %v, want ErrContextCancelled",
			err,
		)
	}
}

func forgetOrUpdate(
	ctx context.Context,
	t *testing.T,
	ex *CommandExecutor[order],
	i int,
) {
	if i == forgetWorker {
		requireForgotten(
			ctx,
			t,
			ex,
		)
		return
	}

	_, err := ex.Execute(
		ctx,
		update(
			"agg",
			float64(i),
		),
	)
	if err != nil &&
		!errors.Is(
			err,
			asynxmd.ErrForgetInProgress,
		) {
		t.Errorf(
			"Execute: %v",
			err,
		)
	}
}

func requireForgotten(
	ctx context.Context,
	t *testing.T,
	ex *CommandExecutor[order],
) {
	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Errorf(
			"Forget: %v",
			err,
		)
	}
}

const forgetWorker = 6

func requireNoGaps(
	t *testing.T,
	blobs [][]byte,
) {
	t.Helper()
	for i, blob := range blobs {
		requireVersionAt(
			t,
			blob,
			i+1,
		)
	}
}

func requireVersionAt(
	t *testing.T,
	blob []byte,
	position int,
) {
	t.Helper()
	var evt esmodels.InternalEvent
	if err := json.Unmarshal(
		blob,
		&evt,
	); err != nil {
		t.Fatal(err)
	}

	if evt.Version != int64(position) {
		t.Fatalf(
			"stream has version %d at position %d: history is gapped",
			evt.Version,
			position,
		)
	}
}

func forgetOrUpdateDone(
	ctx context.Context,
	t *testing.T,
	ex *CommandExecutor[order],
	wg *sync.WaitGroup,
	i int,
) {
	defer wg.Done()
	forgetOrUpdate(
		ctx,
		t,
		ex,
		i,
	)
}

func TestForget_ConcurrentWithUpdatesNeverLeavesAGappedStream(
	t *testing.T,
) {
	for range 20 {
		runConcurrentForgetRound(t)
	}
}

func runConcurrentForgetRound(
	t *testing.T,
) {
	ex, s, _, _, _ := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range 12 {
		wg.Add(1)
		go forgetOrUpdateDone(
			ctx,
			t,
			ex,
			&wg,
			i,
		)
	}

	wg.Wait()

	blobs, _ := s.ReadFrom(
		ctx,
		"events:agg",
		1,
	)
	requireNoGaps(
		t,
		blobs,
	)
}

func matchTombstone(
	name string,
) bool {
	return name == "asynx.aggregate.forget"
}

func matchNothing(
	string,
) bool {
	return false
}

func failNever(
	context.Context,
	asynxmd.Tx,
	asynxmd.Event[order],
) error {
	return errors.New("never")
}

func TestForget_RunsSyncProjectionsForTheTombstone(
	t *testing.T,
) {
	var names []string
	record := func(
		_ context.Context,
		_ asynxmd.Tx,
		e asynxmd.Event[order],
	) error {
		names = append(
			names,
			e.EventName,
		)
		return nil
	}
	ex, _, _, _, _ := newEnv(
		t,
		WithSyncProjections[order]([]models.SyncProjection[order]{{
			Match: matchTombstone,
			Fn:    record,
		}, {
			Match: matchNothing,
			Fn:    failNever,
		}}),
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	if len(names) != 1 ||
		names[0] != "asynx.aggregate.forget" {
		t.Fatalf(
			"projection saw %v, want the tombstone only",
			names,
		)
	}
}

func readModelState(
	down bool,
) error {
	if down {
		return errors.New("read model down")
	}

	return nil
}

func TestForget_SyncProjectionFailureAbortsBeforeErasing(
	t *testing.T,
) {
	failing := false
	projection := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		return readModelState(failing)
	}
	ex, s, _, _, _ := newEnv(
		t,
		syncAll(projection),
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	failing = true
	err := ex.Forget(
		ctx,
		"agg",
	)
	if !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			asynxmd.ErrSyncProjection,
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed + ErrSyncProjection",
			err,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 1 {
		t.Fatalf(
			"stream length = %d, want 1 (nothing erased, tombstone rolled back)",
			n,
		)
	}
}

func TestForget_WithoutDispatcherStillErases(
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
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 0 {
		t.Fatalf(
			"stream length = %d, want 0",
			n,
		)
	}
}

func TestForget_AfterDispatcherClosedFailsPipeline(
	t *testing.T,
) {
	ex, _, _, d, _ := newEnv(
		t,
		WithDurableDelivery[order](forgetAlways),
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	d.Close(ctx)
	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}

	if got := parked(d); !slices.Equal(
		got,
		[]string{"agg"},
	) {
		t.Fatalf(
			"halted = %v, want [agg]",
			got,
		)
	}

	if pending, _ := ex.es.Store().PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"pending = %v, want the marker",
			pending,
		)
	}
}

func TestForget_WithoutDurableDeliveryWritesNoMarkerEntry(
	t *testing.T,
) {
	ex, s, _, d, _ := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	d.Close(ctx)
	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v",
			err,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 0 {
		t.Fatalf(
			"pending = %v, want none",
			pending,
		)
	}
}

func hammer(
	ctx context.Context,
	ex *CommandExecutor[order],
	returned *atomic.Bool,
	stop <-chan struct{},
	bad chan<- error,
) {
	defer close(bad)
	for running := true; running && !isClosed(stop); {
		running = hammerStep(
			ctx,
			ex,
			returned,
			bad,
		)
	}
}

func hammerStep(
	ctx context.Context,
	ex *CommandExecutor[order],
	returned *atomic.Bool,
	bad chan<- error,
) bool {
	err := hammerOnce(
		ctx,
		ex,
		returned,
	)
	if err != nil {
		bad <- err
	}

	return err == nil
}

func isClosed(
	stop <-chan struct{},
) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func hammerOnce(
	ctx context.Context,
	ex *CommandExecutor[order],
	returned *atomic.Bool,
) error {
	after := returned.Load()
	_, err := ex.Execute(
		ctx,
		update(
			"agg",
			2,
		),
	)
	if after &&
		errors.Is(
			err,
			asynxmd.ErrForgetInProgress,
		) {
		return err
	}

	return nil
}

func TestForget_NoCommandIsRefusedAfterItReturns(
	t *testing.T,
) {
	for range 200 {
		runRefusalRound(t)
	}
}

func runRefusalRound(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	var returned atomic.Bool
	bad := make(
		chan error,
		1,
	)
	stop := make(chan struct{})
	go hammer(
		ctx,
		ex,
		&returned,
		stop,
		bad,
	)
	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	returned.Store(true)
	runtime.Gosched()
	close(stop)
	if err := <-bad; err != nil {
		t.Fatalf(
			"command refused after Forget returned: %v",
			err,
		)
	}
}

type snapshotDeleteFailStore struct{ asynxmd.SnapshotStore }

// Delete is the test double implementation for the code under test.
func (snapshotDeleteFailStore) Delete(
	context.Context,
	string,
) error {
	return errors.New("snap gone")
}

func TestForget_SnapshotDeleteFailureReturnsErrForgetFailed(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		snapshotDeleteFailStore{store.NewSnapshots()},
		nil,
		1,
		nil,
	)
	ex := New[order](
		es,
		nil,
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}
}

func reactToFirstVersion(
	ctx context.Context,
	ex *CommandExecutor[order],
	e asynxmd.Event[order],
	started chan<- struct{},
	release <-chan struct{},
) {
	if e.Version != 1 {
		return
	}

	close(started)
	<-release
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			99,
		),
	); err != nil {
		return
	}
}

func forgetAsync(
	ctx context.Context,
	ex *CommandExecutor[order],
	id string,
) <-chan error {
	out := make(
		chan error,
		1,
	)
	go deliverForget(
		ctx,
		ex,
		id,
		out,
	)
	return out
}

func deliverForget(
	ctx context.Context,
	ex *CommandExecutor[order],
	id string,
	out chan<- error,
) {
	out <- ex.Forget(
		ctx,
		id,
	)
}

func requireForgetDone(
	t *testing.T,
	done <-chan error,
	timeoutMsg string,
) {
	t.Helper()
	select {
	case err := <-done:
		requireNoError(
			t,
			"Forget",
			err,
		)
	case <-time.After(forgetTimeout):
		t.Fatal(timeoutMsg)
	}
}

func requireNoError(
	t *testing.T,
	what string,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf(
			"%s: %v",
			what,
			err,
		)
	}
}

const forgetTimeout = 5 * time.Second

func TestForget_DoesNotDeadlockWithAReactionToItsOwnAggregate(
	t *testing.T,
) {
	ex, s, _, _, b := newEnv(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	react := func(
		hctx context.Context,
		e asynxmd.Event[order],
	) {
		reactToFirstVersion(
			hctx,
			ex,
			e,
			started,
			release,
		)
	}
	b.Subscribe(
		"OrderUpdated",
		react,
	)

	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	<-started

	forgot := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	for !ex.isForgetting("agg") {
		runtime.Gosched()
	}

	if err := ex.WithLock(
		ctx,
		"agg",
		func() error { return nil },
	); err != nil {
		t.Fatal(err)
	}

	close(release)

	requireForgetDone(
		t,
		forgot,
		"Forget deadlocked against a reaction to its own aggregate",
	)

	if n := streamLen(
		t,
		s,
		"agg",
	); n != 0 {
		t.Fatalf(
			"stream length = %d, want 0",
			n,
		)
	}
}

func TestForget_SnapshotIsDeletedBeforeTheStream(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		deleteFailStore{s},
		snapshotDeleteFailStore{store.NewSnapshots()},
		nil,
		1,
		nil,
	)
	ex := New[order](
		es,
		nil,
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}

	if n := streamLen(
		t,
		s,
		"agg",
	); n == 0 {
		t.Fatal("stream was erased although the snapshot could not be deleted")
	}
}

func TestWithLock_HonoursContext(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ran := false
	if err := ex.WithLock(
		context.Background(),
		"agg",
		func() error { ran = true; return nil },
	); err != nil ||
		!ran {
		t.Fatalf(
			"WithLock = %v, ran = %v",
			err,
			ran,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.WithLock(
		ctx,
		"agg",
		func() error { t.Fatal("ran"); return nil },
	); !errors.Is(
		err,
		asynxmd.ErrContextCancelled,
	) {
		t.Fatalf(
			"err = %v, want ErrContextCancelled",
			err,
		)
	}
}

func TestForget_SecondConcurrentForgetIsRefusedWhileOneRuns(
	t *testing.T,
) {
	ex, _, _, _, b := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	hold := func(
		context.Context,
		asynxmd.Event[order],
	) {
		close(entered)
		<-release
	}
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		hold,
	)

	first := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	<-entered

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetInProgress,
	) {
		t.Fatalf(
			"second Forget err = %v, want ErrForgetInProgress",
			err,
		)
	}

	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			2,
		),
	); !errors.Is(
		err,
		asynxmd.ErrForgetInProgress,
	) {
		t.Fatalf(
			"Execute err = %v, want ErrForgetInProgress",
			err,
		)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf(
			"first Forget: %v",
			err,
		)
	}
}

func TestForget_AbortsWhenTheCallerGivesUpWaitingForHandlers(
	t *testing.T,
) {
	ex, s, _, _, b := newEnv(t)
	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	hold := func(
		context.Context,
		asynxmd.Event[order],
	) {
		close(entered)
		<-release
	}
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		hold,
	)

	ctx, cancel := context.WithCancel(context.Background())
	got := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	<-entered
	cancel()
	if err := <-got; !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}

	close(release)

	if n := streamLen(
		t,
		s,
		"agg",
	); n == 0 {
		t.Fatal("stream erased although Forget aborted")
	}

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			2,
		),
	); errors.Is(
		err,
		asynxmd.ErrForgetInProgress,
	) {
		t.Fatal("aggregate still fenced after Forget aborted")
	}
}

func TestForget_FailsWhenTheDispatcherIsClosed(
	t *testing.T,
) {
	ex, s, _, d, _ := newEnv(t)
	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	d.Close(context.Background())

	if err := ex.Forget(
		context.Background(),
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			2,
		),
	); errors.Is(
		err,
		asynxmd.ErrForgetInProgress,
	) {
		t.Fatal("aggregate left fenced by a Forget that failed to enqueue")
	}

	_ = s
}

func durableEnv(
	t *testing.T,
	handler asynxmd.DurableHandler[order],
	edits ...func(*asynxmd.DispatcherConfig[order]),
) *CommandExecutor[order] {
	t.Helper()
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	cfg := asynxmd.DispatcherConfig[order]{
		Bus: bus.New[order](),
		DurableSubs: []asynxmd.DurableSub[order]{{
			Match:   forgetAlways,
			Handler: handler,
		}},
		RetryInitial: time.Millisecond,
		RetryMax:     2 * time.Millisecond,
		MaxAttempts:  2,
	}
	for _, edit := range edits {
		edit(&cfg)
	}

	d := dispatcher.Dispatcher(cfg)
	t.Cleanup(func() { d.Close(context.Background()) })
	return New(
		es,
		d,
		WithDurableDelivery[order](forgetAlways),
	)
}

func refuseTombstone(
	_ context.Context,
	e asynxmd.Event[order],
) error {
	if e.EventName == asynxmd.ForgetEventName {
		return errors.New("cannot drop state")
	}

	return nil
}

func TestForget_DoesNotEraseWhenADurableSubscriberCannotTakeTheTombstone(
	t *testing.T,
) {
	ex := durableEnv(
		t,
		refuseTombstone,
	)
	ctx := context.Background()
	if _, err := ex.ExecuteWait(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed wrapping ErrDeliveryParked",
			err,
		)
	}

	if n := streamLen(
		t,
		ex.es.Store().(store.Memory),
		"agg",
	); n == 0 {
		t.Fatal("aggregate erased although a durable subscriber never took the tombstone")
	}

	if got := parked(ex.dispatcher); !slices.Equal(
		got,
		[]string{"agg"},
	) {
		t.Fatalf(
			"halted = %v, want [agg]: later events must wait behind the marker",
			got,
		)
	}

	pending, _ := ex.es.Store().PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 1 ||
		pending[0].Version != 2 {
		t.Fatalf(
			"pending = %v, want only the marker (v2)",
			pending,
		)
	}
}

func forgetOwnAggregate(
	ctx context.Context,
	ex *CommandExecutor[order],
	e asynxmd.Event[order],
	got chan<- error,
) error {
	if e.EventName != asynxmd.ForgetEventName {
		got <- ex.Forget(
			ctx,
			e.AggregateID,
		)
	}

	return nil
}

func TestForget_FromAHandlerOfTheSameAggregateFailsFast(
	t *testing.T,
) {
	var ex *CommandExecutor[order]
	got := make(
		chan error,
		1,
	)
	handler := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) error {
		return forgetOwnAggregate(
			ctx,
			ex,
			e,
			got,
		)
	}
	ex = durableEnv(
		t,
		handler,
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

	if err := <-got; !errors.Is(
		err,
		asynxmd.ErrForgetFromHandler,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFromHandler",
			err,
		)
	}

	if ex.isForgetting("agg") {
		t.Fatal("aggregate left fenced by the refused Forget")
	}
}

func forgetOtherAggregate(
	ctx context.Context,
	ex *CommandExecutor[order],
	e asynxmd.Event[order],
	got chan<- error,
) error {
	if e.EventName != asynxmd.ForgetEventName &&
		e.AggregateID == "a" {
		got <- ex.Forget(
			ctx,
			"b",
		)
	}

	return nil
}

func TestForget_FromAHandlerOfAnotherAggregateIsAllowed(
	t *testing.T,
) {
	var ex *CommandExecutor[order]
	got := make(
		chan error,
		1,
	)
	handler := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) error {
		return forgetOtherAggregate(
			ctx,
			ex,
			e,
			got,
		)
	}
	ex = durableEnv(
		t,
		handler,
	)
	ctx := context.Background()
	if _, err := ex.ExecuteWait(
		ctx,
		update(
			"b",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := ex.ExecuteWait(
		ctx,
		update(
			"a",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := <-got; err != nil {
		t.Fatalf(
			"Forget(b) from a's handler: %v",
			err,
		)
	}
}

func failUnlessTombstone(
	e asynxmd.Event[order],
	attempted chan<- struct{},
) error {
	if e.EventName == asynxmd.ForgetEventName {
		return nil
	}

	attempted <- struct{}{}
	return errors.New("poison")
}

func slowRetries(
	cfg *asynxmd.DispatcherConfig[order],
) {
	cfg.RetryInitial, cfg.RetryMax, cfg.MaxAttempts = time.Hour, time.Hour, 100
}

func TestForget_LetsAFailingDeliveryGoWhenTheTombstoneIsAccepted(
	t *testing.T,
) {
	attempted := make(
		chan struct{},
		8,
	)
	poison := func(
		_ context.Context,
		e asynxmd.Event[order],
	) error {
		return failUnlessTombstone(
			e,
			attempted,
		)
	}
	ex := durableEnv(
		t,
		poison,
		slowRetries,
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	<-attempted

	done := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	requireForgetDone(
		t,
		done,
		"Forget held back by a failing delivery of the same aggregate",
	)
}

type deliveryFailStore struct {
	store.Memory
	drop, enqueue bool
}

// Atomically is the test double implementation for the code under test.
func (s deliveryFailStore) Atomically(
	ctx context.Context,
	fn func(asynxmd.Tx) error,
) error {
	wrap := func(
		tx asynxmd.Tx,
	) error {
		return fn(deliveryFailTx{Tx: tx, drop: s.drop, enqueue: s.enqueue})
	}
	return s.Memory.Atomically(
		ctx,
		wrap,
	)
}

type deliveryFailTx struct {
	asynxmd.Tx
	drop, enqueue bool
}

// DropDeliveries is the test double implementation for the code under test.
func (t deliveryFailTx) DropDeliveries(
	ctx context.Context,
	agg string,
) error {
	if t.drop {
		return errors.New("log gone")
	}

	return t.Tx.DropDeliveries(
		ctx,
		agg,
	)
}

// EnqueueDelivery is the test double implementation for the code under test.
func (t deliveryFailTx) EnqueueDelivery(
	ctx context.Context,
	agg string,
	v int64,
) error {
	if t.enqueue {
		return errors.New("log gone")
	}

	return t.Tx.EnqueueDelivery(
		ctx,
		agg,
		v,
	)
}

func TestForget_DeliveryLogFailureInTheTombstoneTransactionAbortsIt(
	t *testing.T,
) {
	for name, s := range map[string]deliveryFailStore{
		"drop":    {Memory: store.New(), drop: true},
		"enqueue": {Memory: store.New(), enqueue: true},
	} {
		runDeliveryLogFailure(
			t,
			name,
			s,
		)
	}
}

func runDeliveryLogFailure(
	t *testing.T,
	name string,
	s deliveryFailStore,
) {
	run := func(
		t *testing.T,
	) {
		requireTombstoneRollsBack(
			t,
			s,
		)
	}
	t.Run(
		name,
		run,
	)
}

func requireTombstoneRollsBack(
	t *testing.T,
	s deliveryFailStore,
) {
	ex, _, _, _, _ := newEnvOn(
		t,
		s,
		WithDurableDelivery[order](forgetAlways),
	)
	ctx := context.Background()
	if _, err := eventstore.New[order](
		s.Memory,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	).
		Write(
			ctx,
			update(
				"agg",
				1,
			),
		); err != nil {
		t.Fatal(err)
	}

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		asynxmd.ErrPipelineFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed",
			err,
		)
	}

	if n := streamLen(
		t,
		s.Memory,
		"agg",
	); n != 1 {
		t.Fatalf(
			"stream length = %d, want the untouched 1: the tombstone must roll back",
			n,
		)
	}

	if ex.isForgetting("agg") {
		t.Fatal("fence left up after the aborted tombstone")
	}
}

func blockUnlessTombstone(
	e asynxmd.Event[order],
	entered chan<- struct{},
	release <-chan struct{},
) error {
	if e.EventName != asynxmd.ForgetEventName {
		entered <- struct{}{}
		<-release
	}

	return nil
}

func TestForget_GivesUpWhenShutdownBegins(
	t *testing.T,
) {
	entered := make(
		chan struct{},
		1,
	)
	release := make(chan struct{})
	block := func(
		_ context.Context,
		e asynxmd.Event[order],
	) error {
		return blockUnlessTombstone(
			e,
			entered,
			release,
		)
	}
	ex := durableEnv(
		t,
		block,
	)
	ctx := context.Background()
	if _, _, err := ex.ExecuteAsync(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	within(
		t,
		entered,
		"the handler",
	)

	got := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	eventually(
		t,
		"the Forget to raise its fence",
		func() bool { return ex.isForgetting("agg") },
	)
	select {
	case err := <-got:
		t.Fatalf(
			"Forget returned %v while the handler ahead of the tombstone was still running",
			err,
		)
	default:
	}

	t.Cleanup(func() { close(release) })
	ex.dispatcher.Interrupt()
	err := within(
		t,
		got,
		"the Forget to give up",
	)
	if !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			asynxmd.ErrShuttingDown,
		) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed wrapping ErrShuttingDown",
			err,
		)
	}

	if ex.isForgetting("agg") {
		t.Fatal("aggregate left fenced by the abandoned Forget")
	}

	if got := parked(ex.dispatcher); !slices.Equal(
		got,
		[]string{"agg"},
	) {
		t.Fatalf(
			"halted = %v, want [agg]: the marker is still to be delivered",
			got,
		)
	}
}

func TestAwaitTombstone_PrefersADeliveryThatFinished(
	t *testing.T,
) {
	ex, _, _, d, _ := newEnv(t)
	d.Interrupt()
	finished := make(chan struct{})
	close(finished)

	ticket := asynxmd.Ticket{Final: finished, Err: func() error { return nil }}
	for range 50 {
		requireTombstoneAwaited(
			t,
			ex,
			ticket,
		)
	}
}

func requireTombstoneAwaited(
	t *testing.T,
	ex *CommandExecutor[order],
	ticket asynxmd.Ticket,
) {
	t.Helper()
	err := ex.awaitTombstone(
		context.Background(),
		ticket,
	)
	if err != nil {
		t.Fatalf(
			"awaitTombstone = %v for a tombstone that was delivered",
			err,
		)
	}
}

func TestForget_TombstoneEnqueueFailureParksWithTheEnqueueError(
	t *testing.T,
) {
	ex, fake := fakeEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	fake.FailEnqueue(errEnqueue)

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		errEnqueue,
	) {
		t.Fatalf(
			"Forget = %v, want the enqueue error",
			err,
		)
	}

	calls := fake.ParkCalls()
	if len(calls) != 1 ||
		!errors.Is(
			calls[0].Cause,
			errEnqueue,
		) {
		t.Fatalf(
			"ParkCalls = %+v, want one park with the enqueue error",
			calls,
		)
	}
}

func TestForget_ErasePhaseFailureParksWithTheForgetError(
	t *testing.T,
) {
	ex, _, _, d, _ := newEnvOn(
		t,
		deleteFailStore{store.New()},
	)
	ctx := context.Background()
	if _, err := ex.Execute(
		ctx,
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	err := ex.Forget(
		ctx,
		"agg",
	)
	if !errors.Is(
		err,
		asynxmd.ErrForgetFailed,
	) {
		t.Fatalf(
			"Forget = %v, want ErrForgetFailed",
			err,
		)
	}

	if got := d.Inspect("agg"); !got.Parked ||
		got.Cause != err {
		t.Fatalf(
			"Inspect = %+v, want parked with the error Forget returned, %v",
			got,
			err,
		)
	}
}

func holdTombstone(
	e asynxmd.Event[order],
	release <-chan struct{},
) error {
	if e.EventName == asynxmd.ForgetEventName {
		<-release
	}

	return nil
}

func TestForget_GivingUpOnTheTombstoneParksWithTheWaitError(
	t *testing.T,
) {
	release := make(chan struct{})
	hold := func(
		_ context.Context,
		e asynxmd.Event[order],
	) error {
		return holdTombstone(
			e,
			release,
		)
	}
	ex := durableEnv(
		t,
		hold,
	)
	defer close(release)
	if _, err := ex.ExecuteWait(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Millisecond,
	)
	defer cancel()

	err := ex.Forget(
		ctx,
		"agg",
	)
	if !errors.Is(
		err,
		context.DeadlineExceeded,
	) {
		t.Fatalf(
			"Forget = %v, want the context error",
			err,
		)
	}

	if got := ex.dispatcher.Inspect("agg"); !got.Parked ||
		got.Cause != err {
		t.Fatalf(
			"Inspect = %+v, want parked with the error Forget returned, %v",
			got,
			err,
		)
	}
}

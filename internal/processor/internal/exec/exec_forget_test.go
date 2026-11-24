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

	"github.com/char2cs/asynx/v2/internal/bus"
	"github.com/char2cs/asynx/v2/internal/eventstore"
	esmodels "github.com/char2cs/asynx/v2/internal/eventstore/models"
	"github.com/char2cs/asynx/v2/internal/mocks"
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/dispatch"
	dispatchermodel "github.com/char2cs/asynx/v2/models/dispatcher"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/handler"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/models/subscription"
	"github.com/char2cs/asynx/v2/store"
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
	if err := s.Atomically(
		ctx,
		enqueuingDelivery(
			ctx,
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

	seen := &tombstoneObserver{
		t:     t,
		store: s,
	}
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		seen.observe,
	)

	if err := ex.Forget(
		ctx,
		"agg",
	); err != nil {
		t.Fatal(err)
	}

	if seen.total != 42 {
		t.Errorf(
			"tombstone Aggregate.Total = %v, want 42",
			seen.total,
		)
	}

	if seen.streamAtHandler == 0 {
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
		failure.ErrValidation,
	) {
		t.Fatalf(
			"err = %v, want ErrValidation",
			err,
		)
	}
}

type tombstoneObserver struct {
	t               *testing.T
	store           store.Memory
	total           float64
	streamAtHandler int
}

func (o *tombstoneObserver) observe(
	_ context.Context,
	e event.Event[order],
) {
	o.total = e.Aggregate.Total
	o.streamAtHandler = streamLen(
		o.t,
		o.store,
		"agg",
	)
}

type deleteFailStore struct{ store.Memory }

type deleteFailRun func(
	storage.Tx,
) error

func (r deleteFailRun) inFailingTx(
	tx storage.Tx,
) error {
	return r(deleteFailTx{tx})
}

// Atomically runs fn in a transaction whose DeleteStream fails, simulating a
// store that cannot erase a stream.
func (s deleteFailStore) Atomically(
	ctx context.Context,
	fn func(storage.Tx) error,
) error {
	return s.Memory.Atomically(
		ctx,
		deleteFailRun(fn).inFailingTx,
	)
}

type deleteFailTx struct{ storage.Tx }

// DeleteStream always fails with "disk gone".
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
		failure.ErrForgetFailed,
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
		failure.ErrForgetFailed,
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
		failure.ErrContextCancelled,
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
			failure.ErrForgetInProgress,
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
	storage.Tx,
	event.Event[order],
) error {
	return errors.New("never")
}

func TestForget_RunsSyncProjectionsForTheTombstone(
	t *testing.T,
) {
	projected := &projectedEventNames{}
	ex, _, _, _, _ := newEnv(
		t,
		WithSyncProjections[order]([]models.SyncProjection[order]{{
			Match: matchTombstone,
			Fn:    projected.record,
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

	if len(projected.names) != 1 ||
		projected.names[0] != "asynx.aggregate.forget" {
		t.Fatalf(
			"projection saw %v, want the tombstone only",
			projected.names,
		)
	}
}

type projectedEventNames struct {
	names []string
}

func (p *projectedEventNames) record(
	_ context.Context,
	_ storage.Tx,
	e event.Event[order],
) error {
	p.names = append(
		p.names,
		e.EventName,
	)
	return nil
}

type switchableReadModel struct {
	down bool
}

func (m *switchableReadModel) project(
	context.Context,
	storage.Tx,
	event.Event[order],
) error {
	if m.down {
		return errors.New("read model down")
	}

	return nil
}

func TestForget_SyncProjectionFailureAbortsBeforeErasing(
	t *testing.T,
) {
	readModel := &switchableReadModel{}
	ex, s, _, _, _ := newEnv(
		t,
		syncAll(readModel.project),
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

	readModel.down = true
	err := ex.Forget(
		ctx,
		"agg",
	)
	if !errors.Is(
		err,
		failure.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			failure.ErrSyncProjection,
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
		failure.ErrPipelineFailed,
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
		failure.ErrPipelineFailed,
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
			failure.ErrForgetInProgress,
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

type snapshotDeleteFailStore struct{ storage.SnapshotStore }

// Delete always fails with "snap gone", simulating a snapshot store that
// cannot delete a snapshot.
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
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}
}

type firstVersionReaction struct {
	executor *CommandExecutor[order]
	started  chan struct{}
	release  chan struct{}
	outcome  chan error
}

func newFirstVersionReaction(
	ex *CommandExecutor[order],
) *firstVersionReaction {
	return &firstVersionReaction{
		executor: ex,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		outcome: make(
			chan error,
			1,
		),
	}
}

func (r *firstVersionReaction) react(
	ctx context.Context,
	e event.Event[order],
) {
	if e.Version != 1 {
		return
	}

	close(r.started)
	<-r.release
	_, err := r.executor.Execute(
		ctx,
		update(
			"agg",
			99,
		),
	)
	r.outcome <- err
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

	reaction := newFirstVersionReaction(ex)
	b.Subscribe(
		"OrderUpdated",
		reaction.react,
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

	<-reaction.started

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
		returnsNil,
	); err != nil {
		t.Fatal(err)
	}

	close(reaction.release)

	requireForgetDone(
		t,
		forgot,
		"Forget deadlocked against a reaction to its own aggregate",
	)

	if err := <-reaction.outcome; !errors.Is(
		err,
		failure.ErrForgetInProgress,
	) {
		t.Fatalf(
			"the reaction's Execute = %v, want ErrForgetInProgress",
			err,
		)
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
		failure.ErrForgetFailed,
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

func returnsNil(
	context.Context,
) error {
	return nil
}

type runRecorder struct {
	ran bool
}

func (r *runRecorder) record(
	context.Context,
) error {
	r.ran = true
	return nil
}

type mustNotRun struct {
	t *testing.T
}

func (m *mustNotRun) fail(
	context.Context,
) error {
	m.t.Fatal("ran")
	return nil
}

func TestWithLock_HonoursContext(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ran := &runRecorder{}
	if err := ex.WithLock(
		context.Background(),
		"agg",
		ran.record,
	); err != nil ||
		!ran.ran {
		t.Fatalf(
			"WithLock = %v, ran = %v",
			err,
			ran.ran,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ex.WithLock(
		ctx,
		"agg",
		(&mustNotRun{t: t}).fail,
	); !errors.Is(
		err,
		failure.ErrContextCancelled,
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

	gate := newHandlerGate()
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		gate.hold,
	)

	first := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	<-gate.entered

	if err := ex.Forget(
		ctx,
		"agg",
	); !errors.Is(
		err,
		failure.ErrForgetInProgress,
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
		failure.ErrForgetInProgress,
	) {
		t.Fatalf(
			"Execute err = %v, want ErrForgetInProgress",
			err,
		)
	}

	close(gate.release)
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

	gate := newHandlerGate()
	b.Subscribe(
		"asynx\\.aggregate\\.forget",
		gate.hold,
	)

	ctx, cancel := context.WithCancel(context.Background())
	got := forgetAsync(
		ctx,
		ex,
		"agg",
	)
	<-gate.entered
	cancel()
	if err := <-got; !errors.Is(
		err,
		failure.ErrForgetFailed,
	) {
		t.Fatalf(
			"err = %v, want ErrForgetFailed",
			err,
		)
	}

	close(gate.release)

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
		failure.ErrForgetInProgress,
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
		failure.ErrPipelineFailed,
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
		failure.ErrForgetInProgress,
	) {
		t.Fatal("aggregate left fenced by a Forget that failed to enqueue")
	}

	_ = s
}

func durableEnv(
	t *testing.T,
	callback handler.Durable[order],
	edits ...func(*dispatchermodel.Config[order]),
) *CommandExecutor[order] {
	t.Helper()
	es := eventstore.New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	cfg := dispatchermodel.Config[order]{
		Bus: bus.New[order](),
		DurableSubs: []subscription.Durable[order]{{
			Match:   forgetAlways,
			Handler: callback,
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
	e event.Event[order],
) error {
	if e.EventName == event.ForgetEventName {
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
		failure.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			failure.ErrDeliveryParked,
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

type forgetsItsOwnAggregate struct {
	executor *CommandExecutor[order]
	got      chan error
}

func (h *forgetsItsOwnAggregate) handle(
	ctx context.Context,
	e event.Event[order],
) error {
	if e.EventName != event.ForgetEventName {
		h.got <- h.executor.Forget(
			ctx,
			e.AggregateID,
		)
	}

	return nil
}

func TestForget_FromAHandlerOfTheSameAggregateFailsFast(
	t *testing.T,
) {
	reaction := &forgetsItsOwnAggregate{got: make(
		chan error,
		1,
	)}
	ex := durableEnv(
		t,
		reaction.handle,
	)
	reaction.executor = ex
	if _, err := ex.ExecuteWait(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if err := <-reaction.got; !errors.Is(
		err,
		failure.ErrForgetFromHandler,
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

type forgetsAnotherAggregate struct {
	executor *CommandExecutor[order]
	got      chan error
}

func (h *forgetsAnotherAggregate) handle(
	ctx context.Context,
	e event.Event[order],
) error {
	if e.EventName != event.ForgetEventName &&
		e.AggregateID == "a" {
		h.got <- h.executor.Forget(
			ctx,
			"b",
		)
	}

	return nil
}

func TestForget_FromAHandlerOfAnotherAggregateIsAllowed(
	t *testing.T,
) {
	reaction := &forgetsAnotherAggregate{got: make(
		chan error,
		1,
	)}
	ex := durableEnv(
		t,
		reaction.handle,
	)
	reaction.executor = ex
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

	if err := <-reaction.got; err != nil {
		t.Fatalf(
			"Forget(b) from a's handler: %v",
			err,
		)
	}
}

type failsUnlessTombstone struct {
	attempted chan struct{}
}

func (h *failsUnlessTombstone) handle(
	_ context.Context,
	e event.Event[order],
) error {
	if e.EventName == event.ForgetEventName {
		return nil
	}

	h.attempted <- struct{}{}
	return errors.New("poison")
}

func slowRetries(
	cfg *dispatchermodel.Config[order],
) {
	cfg.RetryInitial, cfg.RetryMax, cfg.MaxAttempts = time.Hour, time.Hour, 100
}

func TestForget_LetsAFailingDeliveryGoWhenTheTombstoneIsAccepted(
	t *testing.T,
) {
	poison := &failsUnlessTombstone{attempted: make(
		chan struct{},
		8,
	)}
	ex := durableEnv(
		t,
		poison.handle,
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

	<-poison.attempted

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

// Atomically runs fn in a transaction whose delivery-log operations fail as
// configured by drop and enqueue.
func (s deliveryFailStore) Atomically(
	ctx context.Context,
	fn func(storage.Tx) error,
) error {
	failing := deliveryFailRun{
		run:     fn,
		drop:    s.drop,
		enqueue: s.enqueue,
	}
	return s.Memory.Atomically(
		ctx,
		failing.inFailingTx,
	)
}

type deliveryFailRun struct {
	run           func(storage.Tx) error
	drop, enqueue bool
}

func (r deliveryFailRun) inFailingTx(
	tx storage.Tx,
) error {
	return r.run(deliveryFailTx{
		Tx:      tx,
		drop:    r.drop,
		enqueue: r.enqueue,
	})
}

type deliveryFailTx struct {
	storage.Tx
	drop, enqueue bool
}

// DropDeliveries fails with "log gone" when drop is set and otherwise
// delegates to the wrapped transaction.
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

// EnqueueDelivery fails with "log gone" when enqueue is set and otherwise
// delegates to the wrapped transaction.
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
	t.Run(
		name,
		rollbackCase{store: s}.run,
	)
}

type rollbackCase struct {
	store deliveryFailStore
}

func (c rollbackCase) run(
	t *testing.T,
) {
	requireTombstoneRollsBack(
		t,
		c.store,
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
		failure.ErrPipelineFailed,
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

type blocksUnlessTombstone struct {
	entered chan struct{}
	release chan struct{}
}

func (h *blocksUnlessTombstone) handle(
	_ context.Context,
	e event.Event[order],
) error {
	if e.EventName != event.ForgetEventName {
		h.entered <- struct{}{}
		<-h.release
	}

	return nil
}

func TestForget_GivesUpWhenShutdownBegins(
	t *testing.T,
) {
	block := &blocksUnlessTombstone{
		entered: make(
			chan struct{},
			1,
		),
		release: make(chan struct{}),
	}
	ex := durableEnv(
		t,
		block.handle,
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
		block.entered,
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

	t.Cleanup(func() { close(block.release) })
	ex.dispatcher.Interrupt()
	err := within(
		t,
		got,
		"the Forget to give up",
	)
	if !errors.Is(
		err,
		failure.ErrForgetFailed,
	) ||
		!errors.Is(
			err,
			failure.ErrShuttingDown,
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

	ticket := dispatch.Ticket{Final: finished, Err: func() error { return nil }}
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
	ticket dispatch.Ticket,
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
		failure.ErrForgetFailed,
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

type holdsTombstone struct {
	release chan struct{}
}

func (h *holdsTombstone) handle(
	_ context.Context,
	e event.Event[order],
) error {
	if e.EventName == event.ForgetEventName {
		<-h.release
	}

	return nil
}

func TestForget_GivingUpOnTheTombstoneParksWithTheWaitError(
	t *testing.T,
) {
	hold := &holdsTombstone{release: make(chan struct{})}
	ex := durableEnv(
		t,
		hold.handle,
	)
	defer close(hold.release)
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

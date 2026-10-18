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
	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)


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

func TestForget_ErasePhaseFailureParksTheAggregate(t *testing.T) {
	ex, _, _, d, _ := newEnvOn(t, deleteFailStore{store.New()})
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrForgetFailed) {
		t.Fatalf("err = %v", err)
	}
	if got := d.Halted(); !slices.Equal(got, []string{"agg"}) {
		t.Fatalf("halted = %v, want [agg]", got)
	}
	if ex.isForgetting("agg") {
		t.Fatal("fence left up after the failed erase")
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
	ex, _, _, d, _ := newEnv(t, WithDurableDelivery[order](func(string) bool { return true }))
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	d.Close(ctx)
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v, want ErrPipelineFailed", err)
	}
	// The marker is committed and pending, and nothing may be delivered ahead
	// of it.
	if got := d.Halted(); !slices.Equal(got, []string{"agg"}) {
		t.Fatalf("halted = %v, want [agg]", got)
	}
	if pending, _ := ex.es.Store().PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Fatalf("pending = %v, want the marker", pending)
	}
}

// Without durable delivery there is no delivery log to record the marker in.
func TestForget_WithoutDurableDeliveryWritesNoMarkerEntry(t *testing.T) {
	ex, s, _, d, _ := newEnv(t)
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
		t.Fatal(err)
	}
	d.Close(ctx)
	if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Fatalf("err = %v", err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Fatalf("pending = %v, want none", pending)
	}
}

// Once Forget has returned, no command may still be refused for it: the fence
// is lifted before the lock is released.
func TestForget_NoCommandIsRefusedAfterItReturns(t *testing.T) {
	for range 200 {
		ex, _, _, _, _ := newEnv(t)
		ctx := context.Background()
		if _, err := ex.Execute(ctx, update("agg", 1), false); err != nil {
			t.Fatal(err)
		}
		var returned atomic.Bool
		bad := make(chan error, 1)
		stop := make(chan struct{})
		go func() {
			defer close(bad)
			for {
				select {
				case <-stop:
					return
				default:
				}
				after := returned.Load()
				_, err := ex.Execute(ctx, update("agg", 2), false)
				if after && errors.Is(err, asynxmd.ErrForgetInProgress) {
					bad <- err
					return
				}
			}
		}()
		if err := ex.Forget(ctx, "agg"); err != nil {
			t.Fatal(err)
		}
		returned.Store(true)
		runtime.Gosched()
		close(stop)
		if err := <-bad; err != nil {
			t.Fatalf("command refused after Forget returned: %v", err)
		}
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
	if got := ex.dispatcher.Halted(); !slices.Equal(got, []string{"agg"}) {
		t.Fatalf("halted = %v, want [agg]: later events must wait behind the marker", got)
	}
	// The tombstone's transaction dropped the superseded delivery and left the
	// marker itself pending, so a resume or a restart delivers it first.
	pending, _ := ex.es.Store().PendingDeliveries(ctx, 10)
	if len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("pending = %v, want only the marker (v2)", pending)
	}
}

func TestForget_FromAHandlerOfTheSameAggregateFailsFast(t *testing.T) {
	var ex *CommandExecutor[order]
	got := make(chan error, 1)
	ex = durableEnv(t, func(ctx context.Context, e asynxmd.Event[order]) error {
		if e.EventName != asynxmd.ForgetEventName {
			got <- ex.Forget(ctx, e.AggregateID)
		}
		return nil
	})
	if _, err := ex.Execute(context.Background(), update("agg", 1), true); err != nil {
		t.Fatal(err)
	}
	if err := <-got; !errors.Is(err, asynxmd.ErrForgetFromHandler) {
		t.Fatalf("err = %v, want ErrForgetFromHandler", err)
	}
	if ex.isForgetting("agg") {
		t.Fatal("aggregate left fenced by the refused Forget")
	}
}

func TestForget_FromAHandlerOfAnotherAggregateIsAllowed(t *testing.T) {
	var ex *CommandExecutor[order]
	got := make(chan error, 1)
	ex = durableEnv(t, func(ctx context.Context, e asynxmd.Event[order]) error {
		if e.EventName != asynxmd.ForgetEventName && e.AggregateID == "a" {
			got <- ex.Forget(ctx, "b")
		}
		return nil
	})
	ctx := context.Background()
	if _, err := ex.Execute(ctx, update("b", 1), true); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.Execute(ctx, update("a", 1), true); err != nil {
		t.Fatal(err)
	}
	if err := <-got; err != nil {
		t.Fatalf("Forget(b) from a's handler: %v", err)
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

type deliveryFailStore struct {
	store.Memory
	drop, enqueue bool
}

func (s deliveryFailStore) Atomically(ctx context.Context, fn func(asynxmd.Tx) error) error {
	return s.Memory.Atomically(ctx, func(tx asynxmd.Tx) error {
		return fn(deliveryFailTx{Tx: tx, drop: s.drop, enqueue: s.enqueue})
	})
}

type deliveryFailTx struct {
	asynxmd.Tx
	drop, enqueue bool
}

func (t deliveryFailTx) DropDeliveries(ctx context.Context, agg string) error {
	if t.drop {
		return errors.New("log gone")
	}
	return t.Tx.DropDeliveries(ctx, agg)
}

func (t deliveryFailTx) EnqueueDelivery(ctx context.Context, agg string, v int64) error {
	if t.enqueue {
		return errors.New("log gone")
	}
	return t.Tx.EnqueueDelivery(ctx, agg, v)
}

// The tombstone commits atomically with its bookkeeping in the delivery log: if
// the log cannot be written, there is no tombstone at all.
func TestForget_DeliveryLogFailureInTheTombstoneTransactionAbortsIt(t *testing.T) {
	for name, s := range map[string]deliveryFailStore{
		"drop":    {Memory: store.New(), drop: true},
		"enqueue": {Memory: store.New(), enqueue: true},
	} {
		t.Run(name, func(t *testing.T) {
			ex, _, _, _, _ := newEnvOn(t, s, WithDurableDelivery[order](func(string) bool { return true }))
			ctx := context.Background()
			// Seed through the plain store: the executor's own writes go through the failing tx.
			if _, err := eventstore.New[order](s.Memory, store.NewSnapshots(), nil, 1, nil).
				Write(ctx, update("agg", 1)); err != nil {
				t.Fatal(err)
			}
			if err := ex.Forget(ctx, "agg"); !errors.Is(err, asynxmd.ErrPipelineFailed) {
				t.Fatalf("err = %v, want ErrPipelineFailed", err)
			}
			if n := streamLen(t, s.Memory, "agg"); n != 1 {
				t.Fatalf("stream length = %d, want the untouched 1: the tombstone must roll back", n)
			}
			if ex.isForgetting("agg") {
				t.Fatal("fence left up after the aborted tombstone")
			}
		})
	}
}

// A Forget stuck behind a handler that is waiting for it, which is only
// recognised when the handler passes its own context on, must not hold a
// shutdown open: shutting down releases it.
func TestForget_GivesUpWhenShutdownBegins(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	ex := durableEnv(t, func(_ context.Context, e asynxmd.Event[order]) error {
		if e.EventName != asynxmd.ForgetEventName {
			entered <- struct{}{}
			<-release
		}
		return nil
	})
	ctx := context.Background()
	if _, _, err := ex.ExecuteAsync(ctx, update("agg", 1)); err != nil {
		t.Fatal(err)
	}
	<-entered

	got := make(chan error, 1)
	go func() { got <- ex.Forget(ctx, "agg") }()
	for !ex.isForgetting("agg") {
		runtime.Gosched()
	}
	select {
	case err := <-got:
		t.Fatalf("Forget returned %v while the handler ahead of the tombstone was still running", err)
	default:
	}

	ex.dispatcher.Interrupt()
	err := <-got
	if !errors.Is(err, asynxmd.ErrForgetFailed) || !errors.Is(err, asynxmd.ErrShuttingDown) {
		t.Fatalf("err = %v, want ErrForgetFailed wrapping ErrShuttingDown", err)
	}
	close(release)

	if ex.isForgetting("agg") {
		t.Fatal("aggregate left fenced by the abandoned Forget")
	}
	if got := ex.dispatcher.Halted(); !slices.Equal(got, []string{"agg"}) {
		t.Fatalf("halted = %v, want [agg]: the marker is still to be delivered", got)
	}
}

// A tombstone that was delivered counts even if shutdown begins at the same
// moment.
func TestAwaitTombstone_PrefersADeliveryThatFinished(t *testing.T) {
	ex, _, _, d, _ := newEnv(t)
	d.Interrupt()
	finished := make(chan struct{})
	close(finished)

	for range 50 {
		err := ex.awaitTombstone(context.Background(), dispatcher.Ticket{Final: finished, Err: func() error { return nil }})
		if err != nil {
			t.Fatalf("awaitTombstone = %v for a tombstone that was delivered", err)
		}
	}
}

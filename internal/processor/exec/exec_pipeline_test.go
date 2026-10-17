package exec

import (
	"context"
	"errors"
	"fmt"
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

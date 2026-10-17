package exec

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

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
				if _, err := ex.Execute(ctx, update("agg", float64(i)), false); err != nil {
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

package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- SetError ---

func TestSetError_CausesNextAppendToFail(t *testing.T) {
	s := New()
	ctx := context.Background()
	sentinel := errors.New("injected")

	s.SetError("agg1", sentinel)

	err := s.Append(ctx, "agg1", 1, []byte("data"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestSetError_IsOneShot_SubsequentAppendSucceeds(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SetError("agg1", errors.New("once"))

	s.Append(ctx, "agg1", 1, []byte("x")) //nolint:errcheck — expected to fail

	if err := s.Append(ctx, "agg1", 2, []byte("ok")); err != nil {
		t.Errorf("second Append after SetError: %v (want nil)", err)
	}
}

func TestSetError_OnlyAffectsTargetAggregate(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.SetError("agg1", errors.New("fail agg1"))

	if err := s.Append(ctx, "agg2", 1, []byte("data")); err != nil {
		t.Errorf("Append to agg2 failed unexpectedly: %v", err)
	}
}

// --- New ---

func TestNew_ReturnsEmptyStore(t *testing.T) {
	s := New()
	blobs, err := s.ReadFrom(context.Background(), "agg1", 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty store, got %d blobs", len(blobs))
	}
}

// --- Append ---

func TestAppend_StoresBlob(t *testing.T) {
	s := New()
	ctx := context.Background()

	if err := s.Append(ctx, "agg1", 1, []byte("data")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	blobs, _ := s.ReadFrom(ctx, "agg1", 1)
	if len(blobs) != 1 || string(blobs[0]) != "data" {
		t.Errorf("got %v, want [data]", blobs)
	}
}

func TestAppend_DuplicateVersion_ReturnsErrPipelineFailed(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("first")) //nolint:errcheck

	err := s.Append(ctx, "agg1", 1, []byte("second"))
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Errorf("err = %v, want ErrPipelineFailed", err)
	}
}

func TestAppend_DifferentAggregates_DoNotConflict(t *testing.T) {
	s := New()
	ctx := context.Background()

	if err := s.Append(ctx, "agg1", 1, []byte("a")); err != nil {
		t.Fatalf("Append agg1: %v", err)
	}
	if err := s.Append(ctx, "agg2", 1, []byte("b")); err != nil {
		t.Fatalf("Append agg2: %v", err)
	}
}

func TestAppend_VersionsAreIndependentPerAggregate(t *testing.T) {
	s := New()
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		if err := s.Append(ctx, "agg1", i, []byte("x")); err != nil {
			t.Fatalf("Append agg1 v%d: %v", i, err)
		}
	}

	blobs, _ := s.ReadFrom(ctx, "agg1", 1)
	if len(blobs) != 3 {
		t.Errorf("expected 3 blobs, got %d", len(blobs))
	}
}

// --- ReadFrom ---

func TestReadFrom_ReturnsAllBlobsFromVersion(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg1", 2)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v2" || string(blobs[1]) != "v3" {
		t.Errorf("got %v, want [v2 v3]", blobs)
	}
}

func TestReadFrom_FromZero_ReturnsAll(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, _ := s.ReadFrom(ctx, "agg1", 0)
	if len(blobs) != 2 {
		t.Errorf("expected 2 blobs, got %d", len(blobs))
	}
}

func TestReadFrom_UnknownAggregate_ReturnsEmpty(t *testing.T) {
	s := New()
	blobs, err := s.ReadFrom(context.Background(), "ghost", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

func TestReadFrom_VersionBeyondAll_ReturnsEmpty(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck

	blobs, _ := s.ReadFrom(ctx, "agg1", 99)
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

// --- ReadRange ---

func TestReadRange_ReturnsUpToCount(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 2)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v1" || string(blobs[1]) != "v2" {
		t.Errorf("got %v, want [v1 v2]", blobs)
	}
}

func TestReadRange_CountExceedsAvailable_ReturnsAll(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 100)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 2 {
		t.Errorf("expected 2 blobs, got %d", len(blobs))
	}
}

func TestReadRange_FromMidRange(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck

	blobs, _ := s.ReadRange(ctx, "agg1", 2, 10)
	if len(blobs) != 2 {
		t.Fatalf("expected 2 blobs from v2, got %d", len(blobs))
	}
	if string(blobs[0]) != "v2" {
		t.Errorf("first blob = %q, want v2", blobs[0])
	}
}

func TestReadRange_UnknownAggregate_ReturnsEmpty(t *testing.T) {
	s := New()
	blobs, err := s.ReadRange(context.Background(), "ghost", 1, 10)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty, got %d blobs", len(blobs))
	}
}

func TestReadRange_CountZero_ReturnsEmpty(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck

	blobs, err := s.ReadRange(ctx, "agg1", 1, 0)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected empty for count=0, got %d blobs", len(blobs))
	}
}

// --- Out-of-order appends ---

func TestAppend_OutOfOrder_ReadsInVersionOrder(t *testing.T) {
	s := New()
	ctx := context.Background()

	// Append out of order.
	s.Append(ctx, "agg1", 3, []byte("v3")) //nolint:errcheck
	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 3 {
		t.Fatalf("expected 3 blobs, got %d", len(blobs))
	}
	if string(blobs[0]) != "v1" || string(blobs[1]) != "v2" || string(blobs[2]) != "v3" {
		t.Errorf("got %v, want [v1 v2 v3]", blobs)
	}
}

// --- Context cancellation ---

func TestAppend_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Append(ctx, "agg1", 1, []byte("data"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadFrom_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadFrom(ctx, "agg1", 1)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestReadRange_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.ReadRange(ctx, "agg1", 1, 10)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// --- Concurrent access ---

func TestConcurrentAppend_UniqueVersions_AllSucceed(t *testing.T) {
	s := New()
	const n = 50

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			s.Append(context.Background(), "agg1", int64(v), []byte("data")) //nolint:errcheck
		}(i)
	}
	wg.Wait()

	blobs, err := s.ReadFrom(context.Background(), "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != n {
		t.Errorf("expected %d blobs after concurrent appends, got %d", n, len(blobs))
	}
}

func TestConcurrentAppend_SameVersion_ExactlyOneSucceeds(t *testing.T) {
	s := New()
	const n = 20

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		success int
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Append(context.Background(), "agg1", 1, []byte("data"))
			if err == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if success != 1 {
		t.Errorf("expected exactly 1 success, got %d", success)
	}
}

// --- Delete ---

func TestDelete_RemovesAllEntries(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	if err := s.Delete(ctx, "agg1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	blobs, err := s.ReadFrom(ctx, "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom after Delete: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected 0 blobs after Delete, got %d", len(blobs))
	}
}

func TestDelete_NonExistentAggregate_NoError(t *testing.T) {
	s := New()
	if err := s.Delete(context.Background(), "ghost"); err != nil {
		t.Errorf("Delete on non-existent aggregate: %v (want nil)", err)
	}
}

func TestDelete_DoesNotAffectOtherAggregates(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg2", 1, []byte("v2")) //nolint:errcheck

	s.Delete(ctx, "agg1") //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg2", 1)
	if err != nil {
		t.Fatalf("ReadFrom agg2: %v", err)
	}
	if len(blobs) != 1 {
		t.Errorf("expected 1 blob for agg2, got %d", len(blobs))
	}
}

func TestDelete_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Delete(ctx, "agg1"); err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}

// --- transactions ---

func TestAtomically_NativeIsNilForMemory(t *testing.T) {
	s := New()
	err := s.Atomically(context.Background(), func(tx asynxmd.Tx) error {
		if tx.Native() != nil {
			t.Error("Native() should be nil for the in-memory store")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAtomically_CancelledContextDoesNotRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	err := New().Atomically(ctx, func(asynxmd.Tx) error { ran = true; return nil })
	if !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("err = %v, ran = %v; want context.Canceled and no run", err, ran)
	}
}

func TestAtomically_TxMethodsRejectCancelledContext(t *testing.T) {
	s := New()
	live := context.Background()
	dead, cancel := context.WithCancel(live)
	cancel()

	s.Atomically(live, func(tx asynxmd.Tx) error {
		checks := map[string]error{
			"Append":          tx.Append(dead, "a", 1, nil),
			"DeleteStream":    tx.DeleteStream(dead, "a"),
			"DropDeliveries":  tx.DropDeliveries(dead, "a"),
			"PutReceipt":      tx.PutReceipt(dead, "c", asynxmd.Receipt{}),
			"EnqueueDelivery": tx.EnqueueDelivery(dead, "a", 1),
		}
		for name, err := range checks {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s err = %v, want context.Canceled", name, err)
			}
		}
		return nil
	})
}

func TestReads_RejectCancelledContext(t *testing.T) {
	s := New()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := s.GetReceipt(dead, "c"); !errors.Is(err, context.Canceled) {
		t.Errorf("GetReceipt err = %v", err)
	}
	if _, err := s.PendingDeliveries(dead, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("PendingDeliveries err = %v", err)
	}
	if err := s.AckDelivery(dead, "a", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("AckDelivery err = %v", err)
	}
	if err := s.Delete(dead, "a"); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete err = %v", err)
	}
}

func TestAtomically_RollbackRestoresDeletedStreamAndDroppedDeliveries(t *testing.T) {
	s := New()
	ctx := context.Background()
	s.Append(ctx, "s1", 1, []byte("keep"))
	s.Atomically(ctx, func(tx asynxmd.Tx) error { return tx.EnqueueDelivery(ctx, "s1", 1) })

	boom := errors.New("boom")
	err := s.Atomically(ctx, func(tx asynxmd.Tx) error {
		tx.DeleteStream(ctx, "s1")
		tx.DropDeliveries(ctx, "s1")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}

	if blobs, _ := s.ReadFrom(ctx, "s1", 1); len(blobs) != 1 || string(blobs[0]) != "keep" {
		t.Errorf("stream after rollback = %q, want [keep]", blobs)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 1 {
		t.Errorf("pending after rollback = %v, want 1 entry", pending)
	}
}

func TestAtomically_PanicRollsBackAndRepanics(t *testing.T) {
	s := New()
	ctx := context.Background()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic was swallowed")
			}
		}()
		s.Atomically(ctx, func(tx asynxmd.Tx) error {
			tx.Append(ctx, "s1", 1, []byte("x"))
			panic("fn blew up")
		})
	}()

	if blobs, _ := s.ReadFrom(ctx, "s1", 1); len(blobs) != 0 {
		t.Errorf("append survived a panic: %q", blobs)
	}
	// The store stays usable afterwards (the transaction lock was released).
	if err := s.Append(ctx, "s1", 1, []byte("ok")); err != nil {
		t.Fatalf("store unusable after panic: %v", err)
	}
}

func TestAtomically_SetErrorFailsTheTransactionWithThatError(t *testing.T) {
	s := New()
	ctx := context.Background()
	sentinel := errors.New("injected")
	s.SetError("s1", sentinel)

	err := s.Atomically(ctx, func(tx asynxmd.Tx) error {
		tx.EnqueueDelivery(ctx, "s1", 1)
		return tx.Append(ctx, "s1", 1, nil)
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("delivery survived the failed transaction: %v", pending)
	}
}

func TestPutReceipt_DuplicateAcrossTransactionsIsRejected(t *testing.T) {
	s := New()
	ctx := context.Background()
	put := func() error {
		return s.Atomically(ctx, func(tx asynxmd.Tx) error {
			return tx.PutReceipt(ctx, "c1", asynxmd.Receipt{Accepted: true})
		})
	}
	if err := put(); err != nil {
		t.Fatal(err)
	}
	if err := put(); !errors.Is(err, asynxmd.ErrDuplicateReceipt) {
		t.Fatalf("err = %v, want ErrDuplicateReceipt", err)
	}
}

func TestReads_FailOnACancelledContext(t *testing.T) {
	s := New()
	dead, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := s.GetReceipt(dead, "c"); !errors.Is(err, context.Canceled) {
		t.Errorf("GetReceipt = %v, want context.Canceled", err)
	}
	if _, err := s.PendingDeliveries(dead, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("PendingDeliveries = %v, want context.Canceled", err)
	}
	if err := s.AckDelivery(dead, "a", 1); !errors.Is(err, context.Canceled) {
		t.Errorf("AckDelivery = %v, want context.Canceled", err)
	}
}

func TestGetReceipt_ReturnsWhatWasStored(t *testing.T) {
	ctx := context.Background()
	s := New()
	err := s.Atomically(ctx, func(tx asynxmd.Tx) error {
		return tx.PutReceipt(ctx, "c1", asynxmd.Receipt{Accepted: true})
	})
	if err != nil {
		t.Fatal(err)
	}

	r, found, err := s.GetReceipt(ctx, "c1")
	isStored := found && r.Accepted
	if err != nil || !isStored {
		t.Fatalf("GetReceipt = %+v, %v, %v", r, found, err)
	}
	if _, found, _ := s.GetReceipt(ctx, "other"); found {
		t.Fatal("found a receipt that was never stored")
	}
}

func TestAckDelivery_RemovesOnlyTheNamedEntry(t *testing.T) {
	ctx := context.Background()
	s := New()
	err := s.Atomically(ctx, func(tx asynxmd.Tx) error {
		return errors.Join(tx.EnqueueDelivery(ctx, "a", 1), tx.EnqueueDelivery(ctx, "a", 2))
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.AckDelivery(ctx, "a", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.AckDelivery(ctx, "a", 9); err != nil {
		t.Fatalf("acking an unknown entry = %v, want nil", err)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("pending = %v, want only a v2", pending)
	}
}

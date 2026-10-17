// Package storetest is a conformance suite for models.Store implementations.
// Run it from a test in the package that provides the store:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) models.Store { return newMyStore(t) })
//	}
package storetest

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/char2cs/asynx/models"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(t *testing.T) models.Store

var errBoom = errors.New("storetest: boom")

// Run executes the full conformance suite against stores built by newStore.
func Run(
	t *testing.T,
	newStore Factory,
) {
	t.Helper()

	tests := map[string]func(*testing.T, models.Store){
		"AppendCommitsAndReadFromReturnsInOrder":    appendCommitsAndReads,
		"ReadRangeLimitsCount":                      readRangeLimits,
		"DuplicateVersionConflictsAndRollsBack":     duplicateVersionRollsBack,
		"ErrorRollsBackEverything":                  errorRollsBackEverything,
		"ReceiptsAreStoredAndDuplicatesRejected":    receiptsDuplicates,
		"DeliveriesKeepEnqueueOrderAndAckRemoves":   deliveriesOrderAndAck,
		"DropDeliveriesOnlyAffectsThatAggregate":    dropDeliveries,
		"DeleteStreamIsIdempotentAndScoped":         deleteStream,
		"AckOfUnknownDeliveryIsNotAnError":          ackUnknown,
		"DropThenEnqueueInOneTxKeepsTheNewEntry":    dropThenEnqueue,
		"RollbackRestoresDroppedDeliveries":         rollbackRestoresDrop,
		"DeliveryKeyCanBeEnqueuedAgain":             reEnqueueAfterAckOrDrop,
		"AckAfterDropIsNotAnError":                  ackAfterDrop,
		"NativeIsCallable":                          nativeCallable,
		"ReadsOfUnknownStreamAreEmpty":              readsOfUnknown,
		"PendingDeliveriesHonoursLimit":             pendingLimit,
		"CommitInsideAtomicallyIsVisibleAfterwards": committedVisible,
	}

	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			fn(t, newStore(t))
		})
	}
}

func mustAtomically(
	t *testing.T,
	s models.Store,
	fn func(models.Tx) error,
) {
	t.Helper()
	if err := s.Atomically(context.Background(), fn); err != nil {
		t.Fatalf("Atomically: %v", err)
	}
}

func appendCommitsAndReads(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		for v, d := range []string{"a", "b", "c"} {
			if err := tx.Append(ctx, "s1", int64(v+1), []byte(d)); err != nil {
				return err
			}
		}
		return nil
	})

	blobs, err := s.ReadFrom(ctx, "s1", 2)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(blobs) != 2 || string(blobs[0]) != "b" || string(blobs[1]) != "c" {
		t.Fatalf("ReadFrom(2) = %q, want [b c]", blobs)
	}
}

func readRangeLimits(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		for v := int64(1); v <= 4; v++ {
			if err := tx.Append(ctx, "s1", v, []byte{byte('0' + v)}); err != nil {
				return err
			}
		}
		return nil
	})

	blobs, err := s.ReadRange(ctx, "s1", 2, 2)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(blobs) != 2 || string(blobs[0]) != "2" || string(blobs[1]) != "3" {
		t.Fatalf("ReadRange(2,2) = %q, want [2 3]", blobs)
	}
}

func duplicateVersionRollsBack(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error { return tx.Append(ctx, "s1", 1, []byte("first")) })

	err := s.Atomically(ctx, func(tx models.Tx) error {
		if err := tx.EnqueueDelivery(ctx, "s1", 99); err != nil {
			return err
		}
		return tx.Append(ctx, "s1", 1, []byte("second"))
	})
	if !errors.Is(err, models.ErrPipelineFailed) {
		t.Fatalf("duplicate Append err = %v, want ErrPipelineFailed", err)
	}

	blobs, _ := s.ReadFrom(ctx, "s1", 1)
	if len(blobs) != 1 || string(blobs[0]) != "first" {
		t.Fatalf("stream after conflict = %q, want [first]", blobs)
	}
	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 0 {
		t.Fatalf("delivery enqueued in a rolled-back tx survived: %v", pending)
	}
}

func errorRollsBackEverything(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	err := s.Atomically(ctx, func(tx models.Tx) error {
		_ = tx.Append(ctx, "s1", 1, []byte("x"))
		_ = tx.PutReceipt(ctx, "cmd-1", models.Receipt{Accepted: true, EventID: "e1", Version: 1})
		_ = tx.EnqueueDelivery(ctx, "s1", 1)
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Atomically err = %v, want errBoom", err)
	}

	if blobs, _ := s.ReadFrom(ctx, "s1", 1); len(blobs) != 0 {
		t.Errorf("append survived rollback: %q", blobs)
	}
	if _, found, _ := s.GetReceipt(ctx, "cmd-1"); found {
		t.Error("receipt survived rollback")
	}
	if pending, _ := s.PendingDeliveries(ctx, 10); len(pending) != 0 {
		t.Errorf("delivery survived rollback: %v", pending)
	}
}

func receiptsDuplicates(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	want := models.Receipt{Accepted: true, EventID: "e1", Version: 3}
	mustAtomically(t, s, func(tx models.Tx) error { return tx.PutReceipt(ctx, "cmd-1", want) })

	got, found, err := s.GetReceipt(ctx, "cmd-1")
	if err != nil || !found || got != want {
		t.Fatalf("GetReceipt = %+v, %v, %v; want %+v", got, found, err, want)
	}

	err = s.Atomically(ctx, func(tx models.Tx) error {
		return tx.PutReceipt(ctx, "cmd-1", models.Receipt{Rejection: "nope"})
	})
	if !errors.Is(err, models.ErrDuplicateReceipt) {
		t.Fatalf("duplicate PutReceipt err = %v, want ErrDuplicateReceipt", err)
	}

	if _, found, _ := s.GetReceipt(ctx, "missing"); found {
		t.Error("GetReceipt(missing) reported found")
	}
}

func deliveriesOrderAndAck(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		for _, d := range []models.Delivery{{AggregateID: "a", Version: 1}, {AggregateID: "b", Version: 1}, {AggregateID: "a", Version: 2}} {
			if err := tx.EnqueueDelivery(ctx, d.AggregateID, d.Version); err != nil {
				return err
			}
		}
		return nil
	})

	pending, err := s.PendingDeliveries(ctx, 10)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
	want := []models.Delivery{{AggregateID: "a", Version: 1}, {AggregateID: "b", Version: 1}, {AggregateID: "a", Version: 2}}
	if len(pending) != len(want) {
		t.Fatalf("pending = %v, want %v", pending, want)
	}
	for i := range want {
		if pending[i] != want[i] {
			t.Fatalf("pending = %v, want %v", pending, want)
		}
	}

	if err := s.AckDelivery(ctx, "b", 1); err != nil {
		t.Fatalf("AckDelivery: %v", err)
	}
	pending, _ = s.PendingDeliveries(ctx, 10)
	if len(pending) != 2 || pending[0] != (models.Delivery{AggregateID: "a", Version: 1}) || pending[1] != (models.Delivery{AggregateID: "a", Version: 2}) {
		t.Fatalf("pending after ack = %v, want [{a 1} {a 2}]", pending)
	}
}

func dropDeliveries(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		_ = tx.EnqueueDelivery(ctx, "a", 1)
		_ = tx.EnqueueDelivery(ctx, "b", 1)
		_ = tx.EnqueueDelivery(ctx, "a", 2)
		return tx.DropDeliveries(ctx, "a")
	})

	pending, _ := s.PendingDeliveries(ctx, 10)
	if len(pending) != 1 || pending[0] != (models.Delivery{AggregateID: "b", Version: 1}) {
		t.Fatalf("pending = %v, want [{b 1}]", pending)
	}
}

func deleteStream(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		_ = tx.Append(ctx, "s1", 1, []byte("a"))
		return tx.Append(ctx, "s2", 1, []byte("b"))
	})
	mustAtomically(t, s, func(tx models.Tx) error {
		if err := tx.DeleteStream(ctx, "s1"); err != nil {
			return err
		}
		return tx.DeleteStream(ctx, "never-existed")
	})

	if blobs, _ := s.ReadFrom(ctx, "s1", 1); len(blobs) != 0 {
		t.Errorf("s1 not deleted: %q", blobs)
	}
	if blobs, _ := s.ReadFrom(ctx, "s2", 1); len(blobs) != 1 {
		t.Errorf("s2 was affected by deleting s1: %q", blobs)
	}
	mustAtomically(t, s, func(tx models.Tx) error { return tx.Append(ctx, "s1", 1, []byte("again")) })
}

func ackUnknown(
	t *testing.T,
	s models.Store,
) {
	if err := s.AckDelivery(context.Background(), "ghost", 7); err != nil {
		t.Fatalf("AckDelivery(unknown) = %v, want nil", err)
	}
}

// wantPending fails the test unless the pending deliveries are exactly want.
func wantPending(
	t *testing.T,
	s models.Store,
	want ...models.Delivery,
) {
	t.Helper()
	pending, err := s.PendingDeliveries(context.Background(), 100)
	if err != nil || !slices.Equal(pending, want) {
		t.Fatalf("pending = %v (err %v), want %v", pending, err, want)
	}
}

// enqueueIn runs fn on a transaction, failing the test if it does not commit.
func enqueueIn(
	t *testing.T,
	s models.Store,
	fn func(ctx context.Context, tx models.Tx) error,
) {
	t.Helper()
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error { return fn(ctx, tx) })
}

// Forget drops an aggregate's entries and records its tombstone in one
// transaction; the new entry must survive the drop and sort after the others.
func dropThenEnqueue(
	t *testing.T,
	s models.Store,
) {
	enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error {
		return errors.Join(tx.EnqueueDelivery(ctx, "a", 1), tx.EnqueueDelivery(ctx, "b", 1))
	})
	enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error {
		return errors.Join(tx.DropDeliveries(ctx, "a"), tx.EnqueueDelivery(ctx, "a", 2))
	})
	wantPending(t, s, models.Delivery{AggregateID: "b", Version: 1}, models.Delivery{AggregateID: "a", Version: 2})
}

func rollbackRestoresDrop(
	t *testing.T,
	s models.Store,
) {
	enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error { return tx.EnqueueDelivery(ctx, "a", 1) })
	err := s.Atomically(context.Background(), func(tx models.Tx) error {
		return errors.Join(tx.DropDeliveries(context.Background(), "a"), errBoom)
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Atomically = %v, want the callback's error", err)
	}
	wantPending(t, s, models.Delivery{AggregateID: "a", Version: 1})
}

// A forgotten aggregate starts again at version 1: its keys are reused.
func reEnqueueAfterAckOrDrop(
	t *testing.T,
	s models.Store,
) {
	a1 := models.Delivery{AggregateID: "a", Version: 1}
	enqueue := func() {
		enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error { return tx.EnqueueDelivery(ctx, "a", 1) })
	}
	enqueue()
	if err := s.AckDelivery(context.Background(), "a", 1); err != nil {
		t.Fatal(err)
	}
	enqueue()
	wantPending(t, s, a1)
	enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error { return tx.DropDeliveries(ctx, "a") })
	enqueue()
	wantPending(t, s, a1)
}

// A delivery can be acknowledged after the erase dropped its entry.
func ackAfterDrop(
	t *testing.T,
	s models.Store,
) {
	enqueueIn(t, s, func(ctx context.Context, tx models.Tx) error {
		return errors.Join(tx.EnqueueDelivery(ctx, "a", 1), tx.DropDeliveries(ctx, "a"))
	})
	if err := s.AckDelivery(context.Background(), "a", 1); err != nil {
		t.Fatalf("AckDelivery after drop = %v, want nil", err)
	}
}

func nativeCallable(
	t *testing.T,
	s models.Store,
) {
	mustAtomically(t, s, func(tx models.Tx) error {
		_ = tx.Native()
		return nil
	})
}

func readsOfUnknown(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	if blobs, err := s.ReadFrom(ctx, "nope", 1); err != nil || len(blobs) != 0 {
		t.Errorf("ReadFrom(unknown) = %q, %v", blobs, err)
	}
	if blobs, err := s.ReadRange(ctx, "nope", 1, 5); err != nil || len(blobs) != 0 {
		t.Errorf("ReadRange(unknown) = %q, %v", blobs, err)
	}
}

func pendingLimit(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		for v := int64(1); v <= 5; v++ {
			if err := tx.EnqueueDelivery(ctx, "a", v); err != nil {
				return err
			}
		}
		return nil
	})
	pending, _ := s.PendingDeliveries(ctx, 2)
	if len(pending) != 2 || pending[0].Version != 1 || pending[1].Version != 2 {
		t.Fatalf("PendingDeliveries(2) = %v, want versions 1,2", pending)
	}
}

func committedVisible(
	t *testing.T,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		return tx.Append(ctx, "s1", 1, []byte("a"))
	})
	mustAtomically(t, s, func(tx models.Tx) error {
		return tx.Append(ctx, "s1", 2, []byte("b"))
	})
	blobs, _ := s.ReadFrom(ctx, "s1", 1)
	if len(blobs) != 2 {
		t.Fatalf("len = %d, want 2", len(blobs))
	}
}

// Package storetest is a conformance suite for models.Store implementations.
// Run it from a test in the package that provides the store:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) models.Store { return newMyStore(t) })
//	}
package storetest

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/char2cs/asynx/models"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(t *testing.T) models.Store

var errBoom = errors.New("storetest: boom")

// SnapshotFactory returns a fresh, empty snapshot store for one subtest.
type SnapshotFactory func(t *testing.T) models.SnapshotStore

// Run executes the full conformance suite against stores built by newStore.
func Run(
	t *testing.T,
	newStore Factory,
) {
	t.Helper()

	tests := storeScenarios()

	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			fn(t, newStore(t))
		})
	}
}

func mustAtomically(
	t testing.TB,
	s models.Store,
	fn func(models.Tx) error,
) {
	t.Helper()
	if err := s.Atomically(context.Background(), fn); err != nil {
		t.Fatalf("Atomically: %v", err)
	}
}

func appendEach(
	ctx context.Context,
	tx models.Tx,
	streamID string,
	blobs ...[]byte,
) error {
	for i, blob := range blobs {
		if err := tx.Append(ctx, streamID, int64(i+1), blob); err != nil {
			return err
		}
	}
	return nil
}

func enqueueEach(
	ctx context.Context,
	tx models.Tx,
	deliveries ...models.Delivery,
) error {
	for _, d := range deliveries {
		if err := tx.EnqueueDelivery(ctx, d.AggregateID, d.Version); err != nil {
			return err
		}
	}
	return nil
}

func raceAll(
	n int,
	run func(i int) error,
) []error {
	results := make([]error, n)
	var start, finished sync.WaitGroup
	start.Add(1)
	for i := range n {
		finished.Add(1)
		go func() {
			defer finished.Done()
			start.Wait()
			results[i] = run(i)
		}()
	}
	start.Done()
	finished.Wait()
	return results
}

func appendAndHold(
	ctx context.Context,
	tx models.Tx,
	appended chan<- struct{},
	release <-chan struct{},
) error {
	if err := tx.Append(ctx, "s1", 1, []byte("a")); err != nil {
		return err
	}
	close(appended)
	<-release
	return nil
}

func appendCommitsAndReads(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		return appendEach(ctx, tx, "s1", []byte("a"), []byte("b"), []byte("c"))
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
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		return appendEach(ctx, tx, "s1", []byte("1"), []byte("2"), []byte("3"), []byte("4"))
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	want := []models.Delivery{{AggregateID: "a", Version: 1}, {AggregateID: "b", Version: 1}, {AggregateID: "a", Version: 2}}
	mustAtomically(t, s, func(tx models.Tx) error { return enqueueEach(ctx, tx, want...) })

	pending, err := s.PendingDeliveries(ctx, 10)
	if err != nil {
		t.Fatalf("PendingDeliveries: %v", err)
	}
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
	s models.Store,
) {
	if err := s.AckDelivery(context.Background(), "ghost", 7); err != nil {
		t.Fatalf("AckDelivery(unknown) = %v, want nil", err)
	}
}

// wantPending fails the test unless the pending deliveries are exactly want.
func wantPending(
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
	s models.Store,
) {
	mustAtomically(t, s, func(tx models.Tx) error {
		_ = tx.Native()
		return nil
	})
}

func readsOfUnknown(
	t testing.TB,
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
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		return enqueueEach(ctx, tx,
			models.Delivery{AggregateID: "a", Version: 1},
			models.Delivery{AggregateID: "a", Version: 2},
			models.Delivery{AggregateID: "a", Version: 3},
			models.Delivery{AggregateID: "a", Version: 4},
			models.Delivery{AggregateID: "a", Version: 5},
		)
	})
	pending, _ := s.PendingDeliveries(ctx, 2)
	if len(pending) != 2 || pending[0].Version != 1 || pending[1].Version != 2 {
		t.Fatalf("PendingDeliveries(2) = %v, want versions 1,2", pending)
	}
}

func committedVisible(
	t testing.TB,
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

// Appends may arrive in any order; reads return the stream by version.
func outOfOrderAppends(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	for _, v := range []int64{3, 1, 2} {
		mustAtomically(t, s, func(tx models.Tx) error { return tx.Append(ctx, "s1", v, []byte{byte('0' + v)}) })
	}

	all, err := s.ReadFrom(ctx, "s1", 1)
	if err != nil || !slices.Equal(asStrings(all), []string{"1", "2", "3"}) {
		t.Fatalf("ReadFrom(1) = %q, %v; want [1 2 3] in version order", all, err)
	}
	tail, err := s.ReadFrom(ctx, "s1", 2)
	if err != nil || !slices.Equal(asStrings(tail), []string{"2", "3"}) {
		t.Fatalf("ReadFrom(2) = %q, %v; want [2 3]", tail, err)
	}
	window, err := s.ReadRange(ctx, "s1", 2, 1)
	if err != nil || !slices.Equal(asStrings(window), []string{"2"}) {
		t.Fatalf("ReadRange(2, 1) = %q, %v; want [2]", window, err)
	}
}

// Uniqueness of (stream, version) holds under contention: of many writers
// racing for one version, exactly one commits and the rest conflict.
func concurrentSameVersion(
	t testing.TB,
	s models.Store,
) {
	const writers = 16
	ctx := context.Background()
	results := raceAll(writers, func(i int) error {
		return s.Atomically(ctx, func(tx models.Tx) error { return tx.Append(ctx, "s1", 1, []byte{byte(i)}) })
	})

	committed := 0
	for _, err := range results {
		switch {
		case err == nil:
			committed++
		case !errors.Is(err, models.ErrPipelineFailed):
			t.Errorf("losing writer got %v, want ErrPipelineFailed", err)
		}
	}
	if committed != 1 {
		t.Fatalf("%d writers committed version 1, want exactly one", committed)
	}
	if blobs, _ := s.ReadFrom(ctx, "s1", 1); len(blobs) != 1 {
		t.Fatalf("stream holds %d records, want 1", len(blobs))
	}
}

// Concurrent transactions that do not conflict all commit, and nothing is lost.
func concurrentDistinctVersions(
	t testing.TB,
	s models.Store,
) {
	const writers = 20
	ctx := context.Background()
	results := raceAll(writers, func(i int) error {
		v := int64(i + 1)
		return s.Atomically(ctx, func(tx models.Tx) error {
			return errors.Join(tx.Append(ctx, "s1", v, []byte{byte(v)}), tx.EnqueueDelivery(ctx, "s1", v))
		})
	})
	for _, err := range results {
		if err != nil {
			t.Fatalf("a non-conflicting transaction failed: %v", err)
		}
	}

	blobs, err := s.ReadFrom(ctx, "s1", 1)
	if err != nil || len(blobs) != writers {
		t.Fatalf("stream holds %d records (%v), want %d", len(blobs), err, writers)
	}
	for i, blob := range blobs {
		if blob[0] != byte(i+1) {
			t.Fatalf("record %d holds %d, want the stream in version order", i, blob[0])
		}
	}
	if pending, _ := s.PendingDeliveries(ctx, 100); len(pending) != writers {
		t.Fatalf("%d deliveries pending, want %d", len(pending), writers)
	}
}

// A context that is already done fails the operation and changes nothing.
func cancelledContext(
	t testing.TB,
	s models.Store,
) {
	live := context.Background()
	mustAtomically(t, s, func(tx models.Tx) error {
		return errors.Join(tx.Append(live, "s1", 1, []byte("a")), tx.EnqueueDelivery(live, "s1", 1))
	})

	done, cancel := context.WithCancel(live)
	cancel()

	if err := s.Atomically(done, func(tx models.Tx) error { return tx.Append(live, "s1", 2, []byte("b")) }); err == nil {
		t.Error("Atomically succeeded on a cancelled context")
	}
	if _, err := s.ReadFrom(done, "s1", 1); err == nil {
		t.Error("ReadFrom succeeded on a cancelled context")
	}
	if _, err := s.ReadRange(done, "s1", 1, 1); err == nil {
		t.Error("ReadRange succeeded on a cancelled context")
	}
	if _, _, err := s.GetReceipt(done, "cmd"); err == nil {
		t.Error("GetReceipt succeeded on a cancelled context")
	}
	if _, err := s.PendingDeliveries(done, 10); err == nil {
		t.Error("PendingDeliveries succeeded on a cancelled context")
	}
	if err := s.AckDelivery(done, "s1", 1); err == nil {
		t.Error("AckDelivery succeeded on a cancelled context")
	}

	if blobs, _ := s.ReadFrom(live, "s1", 1); len(blobs) != 1 {
		t.Errorf("stream holds %d records, want only the committed one", len(blobs))
	}
	wantPending(t, s, models.Delivery{AggregateID: "s1", Version: 1})
}

// Reads that run while a transaction is open must neither fail nor deadlock the
// transaction (isolation level is the store's choice: they may see the writes
// or block until the commit), and see them for good once it commits.
func readsDuringOpenTransaction(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	appended := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan error, 1)
	go func() {
		committed <- s.Atomically(ctx, func(tx models.Tx) error {
			return appendAndHold(ctx, tx, appended, release)
		})
	}()
	select {
	case <-appended:
	case err := <-committed:
		t.Fatalf("the transaction ended (%v) without running its function", err)
	}

	read := make(chan error, 1)
	go func() {
		_, err := s.ReadFrom(ctx, "s1", 1)
		read <- err
	}()
	var readErr error
	select {
	case readErr = <-read:
		read = nil
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-committed; err != nil {
		t.Fatalf("the open transaction failed to commit: %v", err)
	}
	if read != nil {
		readErr = <-read
	}
	if readErr != nil {
		t.Errorf("a read during an open transaction failed: %v", readErr)
	}
	if blobs, err := s.ReadFrom(ctx, "s1", 1); err != nil || len(blobs) != 1 {
		t.Fatalf("after the commit ReadFrom = %q, %v; want the record", blobs, err)
	}
}

// RunSnapshots executes the conformance suite for models.SnapshotStore
// implementations built by newStore.
func RunSnapshots(
	t *testing.T,
	newStore SnapshotFactory,
) {
	t.Helper()

	tests := snapshotScenarios()

	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			fn(t, newStore(t))
		})
	}
}

func asStrings(blobs [][]byte) []string {
	out := make([]string, len(blobs))
	for i, b := range blobs {
		out[i] = string(b)
	}
	return out
}

func snapshotUnknown(
	t testing.TB,
	s models.SnapshotStore,
) {
	data, found, err := s.Get(context.Background(), "ghost")
	isAbsent := !found && len(data) == 0
	if err != nil || !isAbsent {
		t.Fatalf("Get(unknown) = %q, %v, %v; want not found and no error", data, found, err)
	}
}

func snapshotRoundTrip(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	want := []byte{0, 1, 2, 255, '{', '}'}
	if err := s.Put(ctx, "a", 7, want); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := s.Get(ctx, "a")
	isStored := found && bytes.Equal(got, want)
	if err != nil || !isStored {
		t.Fatalf("Get = %v, %v, %v; want %v", got, found, err, want)
	}
}

func snapshotReplaced(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	for v, data := range []string{"one", "two", "three"} {
		if err := s.Put(ctx, "a", int64(v+1), []byte(data)); err != nil {
			t.Fatalf("Put v%d: %v", v+1, err)
		}
	}
	got, found, err := s.Get(ctx, "a")
	isNewest := found && string(got) == "three"
	if err != nil || !isNewest {
		t.Fatalf("Get = %q, %v, %v; want the newest snapshot only", got, found, err)
	}
}

func snapshotsIndependent(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(ctx, "a", 1, []byte("A"))
	s.Put(ctx, "b", 1, []byte("B"))
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if got, found, _ := s.Get(ctx, "b"); !found || string(got) != "B" {
		t.Fatalf("deleting a's snapshot changed b's: %q, %v", got, found)
	}
}

func snapshotDeleted(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(ctx, "a", 1, []byte("A"))
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := s.Get(ctx, "a"); found {
		t.Fatal("snapshot survived Delete")
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("second Delete = %v, want nil", err)
	}
	if err := s.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("Delete(unknown) = %v, want nil", err)
	}
}

func snapshotPutAfterDelete(
	t testing.TB,
	s models.SnapshotStore,
) {
	ctx := context.Background()
	s.Put(ctx, "a", 5, []byte("old"))
	s.Delete(ctx, "a")
	if err := s.Put(ctx, "a", 1, []byte("fresh")); err != nil {
		t.Fatalf("Put after Delete: %v", err)
	}
	if got, found, _ := s.Get(ctx, "a"); !found || string(got) != "fresh" {
		t.Fatalf("Get = %q, %v; want the snapshot written after the delete", got, found)
	}
}

func snapshotConcurrentPuts(
	t testing.TB,
	s models.SnapshotStore,
) {
	const writers = 16
	ctx := context.Background()
	payload := func(i int) []byte { return []byte{'v', byte('a' + i)} }
	candidates := map[string]struct{}{}
	for i := range writers {
		candidates[string(payload(i))] = struct{}{}
	}
	results := raceAll(writers, func(i int) error { return s.Put(ctx, "a", int64(i+1), payload(i)) })
	for i, err := range results {
		if err != nil {
			t.Errorf("Put v%d: %v", i+1, err)
		}
	}

	got, found, err := s.Get(ctx, "a")
	if err != nil || !found {
		t.Fatalf("Get = %q, %v, %v", got, found, err)
	}
	if _, ok := candidates[string(got)]; !ok {
		t.Fatalf("Get = %q, which no writer wrote (torn write)", got)
	}
}

func snapshotCancelled(
	t testing.TB,
	s models.SnapshotStore,
) {
	live := context.Background()
	s.Put(live, "a", 1, []byte("A"))
	done, cancel := context.WithCancel(live)
	cancel()

	if err := s.Put(done, "a", 2, []byte("B")); err == nil {
		t.Error("Put succeeded on a cancelled context")
	}
	if _, _, err := s.Get(done, "a"); err == nil {
		t.Error("Get succeeded on a cancelled context")
	}
	if err := s.Delete(done, "a"); err == nil {
		t.Error("Delete succeeded on a cancelled context")
	}
	if got, found, _ := s.Get(live, "a"); !found || string(got) != "A" {
		t.Errorf("the snapshot changed after failed operations: %q, %v", got, found)
	}
}

// scenario is one check of the suite, run against a store of type S.
type scenario[S any] func(
	t testing.TB,
	s S,
)

// storeScenarios lists the scenarios of the suite, by name.
func storeScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"AppendCommitsAndReadFromReturnsInOrder":       appendCommitsAndReads,
		"ReadRangeLimitsCount":                         readRangeLimits,
		"DuplicateVersionConflictsAndRollsBack":        duplicateVersionRollsBack,
		"ErrorRollsBackEverything":                     errorRollsBackEverything,
		"ReceiptsAreStoredAndDuplicatesRejected":       receiptsDuplicates,
		"DeliveriesKeepEnqueueOrderAndAckRemoves":      deliveriesOrderAndAck,
		"DropDeliveriesOnlyAffectsThatAggregate":       dropDeliveries,
		"DeleteStreamIsIdempotentAndScoped":            deleteStream,
		"AckOfUnknownDeliveryIsNotAnError":             ackUnknown,
		"DropThenEnqueueInOneTxKeepsTheNewEntry":       dropThenEnqueue,
		"RollbackRestoresDroppedDeliveries":            rollbackRestoresDrop,
		"DeliveryKeyCanBeEnqueuedAgain":                reEnqueueAfterAckOrDrop,
		"AckAfterDropIsNotAnError":                     ackAfterDrop,
		"NativeIsCallable":                             nativeCallable,
		"ReadsOfUnknownStreamAreEmpty":                 readsOfUnknown,
		"PendingDeliveriesHonoursLimit":                pendingLimit,
		"CommitInsideAtomicallyIsVisibleAfterwards":    committedVisible,
		"AppendsOutOfVersionOrderReadInOrder":          outOfOrderAppends,
		"ConcurrentAppendsOfOneVersionAdmitOne":        concurrentSameVersion,
		"ConcurrentAppendsOfDistinctVersionsAllCommit": concurrentDistinctVersions,
		"CancelledContextFailsEveryOperation":          cancelledContext,
		"ReadsDuringAnOpenTransactionDoNotFail":        readsDuringOpenTransaction,
	}
}

// snapshotScenarios lists the scenarios of the suite, by name.
func snapshotScenarios() map[string]scenario[models.SnapshotStore] {
	return map[string]scenario[models.SnapshotStore]{
		"GetOfUnknownAggregateIsNotFoundNotAnError": snapshotUnknown,
		"PutThenGetReturnsTheBytes":                 snapshotRoundTrip,
		"PutReplacesTheSnapshot":                    snapshotReplaced,
		"AggregatesAreIndependent":                  snapshotsIndependent,
		"DeleteRemovesAndIsIdempotent":              snapshotDeleted,
		"PutAfterDeleteStores":                      snapshotPutAfterDelete,
		"ConcurrentPutsLeaveOneOfThem":              snapshotConcurrentPuts,
		"CancelledContextFailsEveryOperation":       snapshotCancelled,
	}
}

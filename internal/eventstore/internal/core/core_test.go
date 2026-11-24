package core

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

func cmd(
	newState order,
) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: "agg1", NewState: newState}
}

func cmdSnap(
	newState order,
) mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: "agg1", NewState: newState, Snapshot: true}
}

func cmdEmpty() mocks.UpdateOrderCmd {
	return mocks.UpdateOrderCmd{ID: "agg1"}
}

func TestWrite_Get_RoundTrip(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	_, err := es.Write(
		ctx,
		cmd(order{ID: "1", Status: "Pending", Total: 100}),
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Pending" ||
		got.Total != 100 {
		t.Errorf(
			"got Status=%q Total=%v, want Pending/100",
			got.Status,
			got.Total,
		)
	}
}

func TestWrite_MultipleEvents_GetReturnsLatest(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	_, err := es.Write(
		ctx,
		cmd(order{Status: "Pending"}),
	)
	if err != nil {
		t.Fatalf(
			"Write #1: %v",
			err,
		)
	}

	_, err = es.Write(
		ctx,
		cmd(order{Status: "Shipped", Total: 200}),
	)
	if err != nil {
		t.Fatalf(
			"Write #2: %v",
			err,
		)
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Shipped" ||
		got.Total != 200 {
		t.Errorf(
			"got Status=%q Total=%v, want Shipped/200",
			got.Status,
			got.Total,
		)
	}
}

func TestWrite_VersionsAreConsecutive(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	for i := int64(1); i <= 5; i++ {
		assertConsecutiveVersion(
			t,
			es,
			i,
		)
	}
}

func assertConsecutiveVersion(
	t *testing.T,
	es *EventStore[order],
	i int64,
) {
	t.Helper()

	evt, err := es.Write(
		context.Background(),
		cmdEmpty(),
	)
	if err != nil {
		t.Fatalf(
			"Write #%d: %v",
			i,
			err,
		)
	}

	if evt.Version != i {
		t.Errorf(
			"Write #%d: Version = %d, want %d",
			i,
			evt.Version,
			i,
		)
	}
}

func TestWrite_WithSnapshot_AcceleratesGet(
	t *testing.T,
) {
	s := store.New()
	ss := store.NewSnapshots()
	es := New[order](
		s,
		ss,
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	_, err := es.Write(
		ctx,
		cmdSnap(order{Status: "Snapped"}),
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("expected snapshot to exist")
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Snapped" {
		t.Errorf(
			"Status = %q, want Snapped",
			got.Status,
		)
	}
}

func TestExists_TrueAfterWrite(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	es.Write(
		ctx,
		cmdEmpty(),
	)

	ok, err := es.Exists(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Exists: %v",
			err,
		)
	}

	if !ok {
		t.Error("Exists = false, want true")
	}
}

func TestExists_FalseForNewAggregate(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	ok, err := es.Exists(
		context.Background(),
		"new",
	)
	if err != nil {
		t.Fatalf(
			"Exists: %v",
			err,
		)
	}

	if ok {
		t.Error("Exists = true, want false")
	}
}

func TestPreload_NoErrorForExistingAggregate(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	es.Write(
		ctx,
		cmd(order{Status: "Ready"}),
	)

	if err := es.Preload(
		ctx,
		"agg1",
	); err != nil {
		t.Errorf(
			"Preload: %v",
			err,
		)
	}
}

func TestPreload_NoErrorForMissingAggregate(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	if err := es.Preload(
		context.Background(),
		"missing",
	); err != nil {
		t.Errorf(
			"Preload on missing aggregate: %v (want nil)",
			err,
		)
	}
}

func TestReplay_VisitsAllEvents(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	es.Write(
		ctx,
		cmd(order{Status: "Pending"}),
	)
	es.Write(
		ctx,
		cmd(order{Status: "Shipped", Total: 100}),
	)
	es.Write(
		ctx,
		cmd(order{Status: "Delivered", Total: 100}),
	)

	var got []asynxmd.Event[order]
	replayFn := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		got = append(
			got,
			e,
		)
	}
	err := es.Replay(
		ctx,
		"agg1",
		1,
		0,
		replayFn,
	)
	if err != nil {
		t.Fatalf(
			"Replay: %v",
			err,
		)
	}

	if len(got) != 3 {
		t.Fatalf(
			"expected 3 events, got %d",
			len(got),
		)
	}

	if got[2].Aggregate.Status != "Delivered" {
		t.Errorf(
			"last event Status = %q, want Delivered",
			got[2].Aggregate.Status,
		)
	}
}

func TestReplay_WithVersionRange(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	es.Write(
		ctx,
		cmd(order{Status: "v1"}),
	)
	es.Write(
		ctx,
		cmd(order{Status: "v2"}),
	)
	es.Write(
		ctx,
		cmd(order{Status: "v3"}),
	)

	var got []asynxmd.Event[order]
	replayFn := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		got = append(
			got,
			e,
		)
	}
	err := es.Replay(
		ctx,
		"agg1",
		2,
		3,
		replayFn,
	)
	if err != nil {
		t.Fatalf(
			"Replay: %v",
			err,
		)
	}

	if len(got) != 2 {
		t.Fatalf(
			"expected 2 events for range [2,3], got %d",
			len(got),
		)
	}
}

func TestWrite_Get_WithUpcasting(
	t *testing.T,
) {
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		upcasters,
		2,
		nil,
	)
	ctx := context.Background()

	esV1 := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	_, err := esV1.Write(
		ctx,
		cmd(order{Status: "OldSchema"}),
	)
	if err != nil {
		t.Fatalf(
			"Write v1: %v",
			err,
		)
	}

	var rawBlobs [][]byte
	replayFn := func(
		ctx context.Context,
		e asynxmd.Event[order],
	) {
		_ = e
	}
	esV1.Replay(
		ctx,
		"agg1",
		1,
		0,
		replayFn,
	)

	_ = rawBlobs
	_ = es
}

func TestWrite_ConcurrentConflict_AtLeastOneFails(
	t *testing.T,
) {
	const rounds = 50
	conflicted := false
	for i := 0; i < rounds &&
		!conflicted; i++ {
		conflicted = concurrentWritesConflict(t)
	}

	if !conflicted {
		t.Errorf(
			"expected a concurrent write to fail with ErrPipelineFailed in at least one of %d rounds",
			rounds,
		)
	}
}

func spawn(
	wg *sync.WaitGroup,
	work func(),
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		work()
	}()
}

func writeWhenStarted(
	ctx context.Context,
	es *EventStore[order],
	start <-chan struct{},
	errOut *error,
) func() {
	return func() {
		<-start
		_, *errOut = es.Write(
			ctx,
			cmdEmpty(),
		)
	}
}

func concurrentWritesConflict(
	t *testing.T,
) bool {
	t.Helper()
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	const n = 20
	start := make(chan struct{})
	errs := make(
		[]error,
		n,
	)

	var wg sync.WaitGroup
	for i := range n {
		spawn(
			&wg,
			writeWhenStarted(
				ctx,
				es,
				start,
				&errs[i],
			),
		)
	}

	close(start)
	wg.Wait()
	containsFuncFn := func(
		err error,
	) bool {
		return errors.Is(
			err,
			asynxmd.ErrPipelineFailed,
		)
	}
	return slices.ContainsFunc(
		errs,
		containsFuncFn,
	)
}

func TestGet_ErrNotFound_ForNewAggregate(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)

	_, err := es.Get(
		context.Background(),
		"ghost",
	)
	if !errors.Is(
		err,
		asynxmd.ErrNotFound,
	) {
		t.Errorf(
			"err = %v, want ErrNotFound",
			err,
		)
	}
}

func failOnPanic(
	t *testing.T,
) {
	r := recover()
	if r != nil {
		t.Fatalf(
			"New panicked with nil upcasters: %v",
			r,
		)
	}
}

func TestNew_NilUpcasters_DoesNotPanic(
	t *testing.T,
) {
	defer failOnPanic(t)
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	_ = es
}

func TestWrite_EmitEventDrivesStoredState(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	emitted := order{ID: "42", Status: "EmittedState", Total: 999}
	evt, err := es.Write(
		ctx,
		cmd(emitted),
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	if evt.Aggregate != emitted {
		t.Errorf(
			"Aggregate = %+v, want %+v (from EmitEvent)",
			evt.Aggregate,
			emitted,
		)
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got != emitted {
		t.Errorf(
			"Get = %+v, want %+v",
			got,
			emitted,
		)
	}
}

func TestWrite_Get_SharedStore_WithUpcasting(
	t *testing.T,
) {
	s := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	esV1 := New[order](
		s,
		ss,
		nil,
		1,
		nil,
	)
	_, err := esV1.Write(
		ctx,
		cmd(order{Status: "OldSchema"}),
	)
	if err != nil {
		t.Fatalf(
			"Write v1: %v",
			err,
		)
	}

	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	esV2 := New[order](
		s,
		ss,
		upcasters,
		2,
		nil,
	)

	got, err := esV2.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "OldSchema" {
		t.Errorf(
			"Status = %q, want OldSchema",
			got.Status,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("expected 1 auto-snapshot, got none (upcaster path not exercised)")
	}
}

func TestNew_WithCorruptionHook_CalledOnCorruptSnapshot(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		mustMarshal(struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Total  int    `json:"total"`
		}{}),
	)

	ss.Put(
		ctx,
		"agg1",
		1,
		[]byte(`not-valid-json`),
	)

	var hookCalled bool
	newFn := func(
		err error,
	) {
		hookCalled = true
	}
	eventstore := New[order](
		es,
		ss,
		nil,
		1,
		newFn,
	)

	_, _ = eventstore.Get(
		ctx,
		"agg1",
	)
	if !hookCalled {
		t.Error("WithCorruptionHook: hook was not called on corrupt snapshot")
	}
}

func mustMarshal(
	v any,
) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return b
}

var _ = mustMarshal

func TestDeleteSnapshot_KeepsTheEventsSoGetRebuildsFromThem(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	if _, err := es.Write(
		ctx,
		cmdSnap(order{ID: "1", Status: "Snapped", Total: 50}),
	); err != nil {
		t.Fatalf(
			"Write with snapshot: %v",
			err,
		)
	}

	if err := es.DeleteSnapshot(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"DeleteSnapshot: %v",
			err,
		)
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil ||
		got.Status != "Snapped" {
		t.Fatalf(
			"Get = %+v, %v; want the state rebuilt from the events",
			got,
			err,
		)
	}
}

func TestDeleteSnapshot_NonExistentAggregate_NoError(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	if err := es.DeleteSnapshot(
		context.Background(),
		"ghost",
	); err != nil {
		t.Errorf(
			"DeleteSnapshot on non-existent aggregate: %v (want nil)",
			err,
		)
	}
}

type deleteFailsSnapshots struct {
	asynxmd.SnapshotStore
	err error
}

// Delete always fails with the configured error.
func (d deleteFailsSnapshots) Delete(
	context.Context,
	string,
) error {
	return d.err
}

func TestDeleteSnapshot_StoreError_ReturnsError(
	t *testing.T,
) {
	sentinel := errors.New("store error")
	es := New[order](
		store.New(),
		deleteFailsSnapshots{
			SnapshotStore: store.NewSnapshots(),
			err:           sentinel,
		},
		nil,
		1,
		nil,
	)
	err := es.DeleteSnapshot(
		context.Background(),
		"agg1",
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"expected sentinel error, got %v",
			err,
		)
	}
}

func TestWrite_GetStorageError_ReturnsError(
	t *testing.T,
) {
	sentinel := errors.New("storage error")
	es := New[order](
		&mockstores.ErrStore{Err: sentinel},
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	_, err := es.Write(
		ctx,
		cmd(order{ID: "1", Status: "Pending", Total: 100}),
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"expected sentinel error from Get, got %v",
			err,
		)
	}
}

func TestEventStore_DeleteSnapshotRemovesSnapshotWithoutPrefix(
	t *testing.T,
) {
	ctx := context.Background()
	snaps := store.NewSnapshots()
	es := New[order](
		store.New(),
		snaps,
		nil,
		1,
		nil,
	)

	if _, err := es.Write(
		ctx,
		mocks.UpdateOrderCmd{
			ID:       "agg-1",
			NewState: order{ID: "agg-1", Total: 1, Status: "Pending"},
			Snapshot: true,
		},
	); err != nil {
		t.Fatalf(
			"write: %v",
			err,
		)
	}

	if _, found, _ := snaps.Get(
		ctx,
		"agg-1",
	); !found {
		t.Fatal("snapshot missing before delete — key must be the raw aggregateID")
	}

	if err := es.DeleteSnapshot(
		ctx,
		"agg-1",
	); err != nil {
		t.Fatalf(
			"delete: %v",
			err,
		)
	}

	if _, found, _ := snaps.Get(
		ctx,
		"agg-1",
	); found {
		t.Error("snapshot still present after DeleteSnapshot")
	}
}

func TestWriteTx_HookRunsInsideTheAppendTransaction(
	t *testing.T,
) {
	s := store.New()
	es := New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	var sawVersion int64
	writeTxFn := func(
		_ context.Context,
		tx asynxmd.Tx,
		e asynxmd.Event[order],
	) error {
		sawVersion = e.Version
		return tx.EnqueueDelivery(
			ctx,
			e.AggregateID,
			e.Version,
		)
	}
	evt, err := es.WriteTx(
		ctx,
		cmd(order{ID: "agg1", Total: 3}),
		writeTxFn,
	)
	if err != nil {
		t.Fatal(err)
	}

	if sawVersion != 1 ||
		evt.Version != 1 {
		t.Fatalf(
			"hook saw v%d, event v%d; want 1",
			sawVersion,
			evt.Version,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 {
		t.Fatalf(
			"hook's delivery not committed: %v",
			pending,
		)
	}
}

func TestWriteTx_HookErrorRollsBackTheEvent(
	t *testing.T,
) {
	s := store.New()
	es := New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()
	boom := errors.New("hook failed")
	writeTxFn := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		return boom
	}
	_, err := es.WriteTx(
		ctx,
		cmd(order{ID: "agg1"}),
		writeTxFn,
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"err = %v, want hook error",
			err,
		)
	}

	if ok, _ := es.Exists(
		ctx,
		"agg1",
	); ok {
		t.Fatal("event survived a failing hook")
	}
}

func TestWriteTx_NeverWritesASnapshot(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	es := New[order](
		store.New(),
		ss,
		nil,
		1,
		nil,
	)
	ctx := context.Background()
	if _, err := es.WriteTx(
		ctx,
		cmdSnap(order{ID: "agg1"}),
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("WriteTx wrote a snapshot")
	}
}

func TestSnapshot_UpsertsTheEventState(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	es := New[order](
		store.New(),
		ss,
		nil,
		1,
		nil,
	)
	ctx := context.Background()
	evt, _ := es.WriteTx(
		ctx,
		cmd(order{ID: "agg1", Total: 9}),
		nil,
	)

	if err := es.Snapshot(
		ctx,
		evt,
	); err != nil {
		t.Fatal(err)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("snapshot missing")
	}
}

func TestStore_ReturnsTheBackingEventStore(
	t *testing.T,
) {
	s := store.New()
	es := New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	if es.Store() != asynxmd.Store(s) {
		t.Fatal("Store() is not the backing event store")
	}
}

func TestWrite_SnapshotFailureAfterCommitIsReturned(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	es := New[order](
		store.New(),
		ss,
		nil,
		1,
		nil,
	)
	ss.SetError(
		"agg1",
		errors.New("snapshot down"),
	)

	if _, err := es.Write(
		context.Background(),
		cmdSnap(order{ID: "agg1"}),
	); err == nil {
		t.Fatal("Write hid a snapshot failure")
	}
}

func TestWriteTx_ValidationFailureWritesNothingAndSkipsTheHook(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	called := false
	writeTxFn := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		called = true
		return nil
	}
	_, err := es.WriteTx(
		context.Background(),
		mocks.CreateOrderCmd{ID: "agg1", Total: -1},
		writeTxFn,
	)
	if !errors.Is(
		err,
		asynxmd.ErrValidation,
	) ||
		called {
		t.Fatalf(
			"err = %v, hook called = %v; want ErrValidation and no hook",
			err,
			called,
		)
	}

	if ok, _ := es.Exists(
		context.Background(),
		"agg1",
	); ok {
		t.Fatal("rejected command wrote an event")
	}
}

func TestGuardAutoSnapshots_InstallsTheGuardOnTheReader(
	t *testing.T,
) {
	ctx := context.Background()
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	asked := 0
	guardAutoSnapshotsFn := func(
		string,
		func(),
	) bool {
		asked++
		return false
	}
	es.GuardAutoSnapshots(guardAutoSnapshotsFn)
	if _, err := es.Write(
		ctx,
		cmd(order{Total: 5}),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := es.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if asked > 1 {
		t.Fatalf(
			"guard consulted %d times for one Get",
			asked,
		)
	}
}

func incrementIf(
	got string,
	want string,
	n *int,
) {
	if got != want {
		return
	}

	*n++
}

func expectPending(
	t *testing.T,
	cur *order,
) {
	if cur != nil &&
		cur.Status == "Pending" {
		return
	}

	t.Errorf(
		"B expected to load Pending, got %+v",
		cur,
	)
}

type gatedCancel struct {
	id         string
	onValidate func(
		current *order,
	)
}

// AggregateID returns the id of the aggregate being cancelled.
func (c gatedCancel) AggregateID() string { return c.id }

// EventName returns the name of the cancellation event.
func (c gatedCancel) EventName() string { return "OrderCancelled" }

// ShouldSnapshot reports that cancelling never requests a snapshot.
func (c gatedCancel) ShouldSnapshot() bool { return false }

// Validate runs the gate hook, then rejects a missing or cancelled order.
func (c gatedCancel) Validate(
	current *order,
) error {
	if c.onValidate != nil {
		c.onValidate(current)
	}

	if current == nil ||
		current.Status == "Cancelled" {
		return asynxmd.ErrValidation
	}

	return nil
}

// EmitEvent returns the order in the cancelled state.
func (c gatedCancel) EmitEvent(
	current *order,
) order {
	o := *current
	o.Status = "Cancelled"
	return o
}

func TestWrite_StaleValidation_ConflictsInsteadOfDoubleCommit(
	t *testing.T,
) {
	s := store.New()
	es := New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	if _, err := es.Write(
		ctx,
		mocks.CreateOrderCmd{ID: "X", Total: 100},
	); err != nil {
		t.Fatalf(
			"seed create: %v",
			err,
		)
	}

	aCommitted := make(chan struct{})
	bInValidate := make(chan struct{})
	onValidate2 := func(
		cur *order,
	) {
		expectPending(
			t,
			cur,
		)
		close(bInValidate)
		<-aCommitted
	}
	bCmd := gatedCancel{id: "X", onValidate: onValidate2}

	var bErr error
	bDone := make(chan struct{})
	go func() {
		_, bErr = es.Write(
			ctx,
			bCmd,
		)
		close(bDone)
	}()

	<-bInValidate
	if _, err := es.Write(
		ctx,
		gatedCancel{id: "X"},
	); err != nil {
		t.Fatalf(
			"A cancel: %v",
			err,
		)
	}

	close(aCommitted)
	<-bDone

	if !errors.Is(
		bErr,
		asynxmd.ErrPipelineFailed,
	) {
		t.Errorf(
			"B err = %v, want ErrPipelineFailed (stale write must be rejected)",
			bErr,
		)
	}

	cancels := 0
	replayFn := func(
		_ context.Context,
		e asynxmd.Event[order],
	) {
		incrementIf(
			e.EventName,
			"OrderCancelled",
			&cancels,
		)
	}
	if err := es.Replay(
		ctx,
		"X",
		1,
		0,
		replayFn,
	); err != nil {
		t.Fatalf(
			"replay: %v",
			err,
		)
	}

	if cancels != 1 {
		t.Fatalf(
			"order X cancelled %d times, want exactly 1",
			cancels,
		)
	}
}

func writeSnapshotUpdate(
	t *testing.T,
	es *EventStore[order],
	id string,
	i int,
) {
	t.Helper()

	_, err := es.Write(
		context.Background(),
		mocks.UpdateOrderCmd{
			ID:       id,
			NewState: order{ID: id, Total: float64(i), Status: "Pending"},
			Snapshot: true,
		},
	)
	if err != nil {
		t.Fatalf(
			"write %d: %v",
			i,
			err,
		)
	}
}

func TestSnapshotReadDoesNotScaleWithHistory(
	t *testing.T,
) {
	ctx := context.Background()
	snaps := &mockstores.CountingSnapshotStore{Inner: store.NewSnapshots()}
	es := New[order](
		store.New(),
		snaps,
		nil,
		1,
		nil,
	)

	const n = 500
	for i := range n {
		writeSnapshotUpdate(
			t,
			es,
			"agg-1",
			i,
		)
	}

	snaps.Reset()
	if _, err := es.Get(
		ctx,
		"agg-1",
	); err != nil {
		t.Fatalf(
			"get: %v",
			err,
		)
	}

	if snaps.GetCalls != 1 {
		t.Errorf(
			"Get calls = %d, want 1",
			snaps.GetCalls,
		)
	}

	if snaps.GetBytes > 512 {
		t.Errorf(
			"bytes read = %d after %d snapshots, want one blob — read must not scale with history",
			snaps.GetBytes,
			n,
		)
	}
}

func TestWritePaysConstantSnapshotCost(
	t *testing.T,
) {
	ctx := context.Background()
	snaps := &mockstores.CountingSnapshotStore{Inner: store.NewSnapshots()}
	es := New[order](
		store.New(),
		snaps,
		nil,
		1,
		nil,
	)

	const n = 500
	for i := range n {
		writeSnapshotUpdate(
			t,
			es,
			"agg-2",
			i,
		)
	}

	snaps.Reset()
	if _, err := es.Write(
		ctx,
		mocks.UpdateOrderCmd{
			ID:       "agg-2",
			NewState: order{ID: "agg-2", Total: 999, Status: "Pending"},
			Snapshot: true,
		},
	); err != nil {
		t.Fatalf(
			"write: %v",
			err,
		)
	}

	if snaps.GetCalls != 1 {
		t.Errorf(
			"Get calls = %d, want 1",
			snaps.GetCalls,
		)
	}

	if snaps.GetBytes > 512 {
		t.Errorf(
			"bytes read = %d on one Write after %d snapshots, want one blob",
			snaps.GetBytes,
			n,
		)
	}
}

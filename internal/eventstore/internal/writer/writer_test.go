package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

func newTestWriter(
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
) *Writer[order] {
	return New[order](
		es,
		ss,
		1,
	)
}

type eventAppend[T any] struct {
	writer          *Writer[T]
	aggregateID     string
	eventName       string
	expectedVersion int64
	previousState   T
	newState        T
	evt             asynxmd.Event[T]
}

func (a *eventAppend[T]) inTransaction(
	ctx context.Context,
) func(asynxmd.Tx) error {
	return func(
		tx asynxmd.Tx,
	) error {
		return a.appendIn(
			ctx,
			tx,
		)
	}
}

func (a *eventAppend[T]) appendIn(
	ctx context.Context,
	tx asynxmd.Tx,
) error {
	evt, err := a.writer.AppendTx(
		ctx,
		tx,
		a.aggregateID,
		a.eventName,
		a.expectedVersion,
		a.previousState,
		a.newState,
	)
	a.evt = evt
	return err
}

func appendEvent[T any](
	ctx context.Context,
	w *Writer[T],
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
) (asynxmd.Event[T], error) {
	pending := &eventAppend[T]{
		writer:          w,
		aggregateID:     aggregateID,
		eventName:       eventName,
		expectedVersion: expectedVersion,
		previousState:   previousState,
		newState:        newState,
	}
	err := w.EventStore().Atomically(
		ctx,
		pending.inTransaction(ctx),
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return pending.evt,
		nil
}

func appendSnapshotted[T any](
	ctx context.Context,
	w *Writer[T],
	aggregateID string,
	eventName string,
	expectedVersion int64,
	previousState T,
	newState T,
) (asynxmd.Event[T], error) {
	evt, err := appendEvent(
		ctx,
		w,
		aggregateID,
		eventName,
		expectedVersion,
		previousState,
		newState,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	err = w.Snapshot(
		ctx,
		evt,
	)
	if err != nil {
		return asynxmd.Event[T]{},
			err
	}

	return evt,
		nil
}

func TestAppendTx_AppendsEventAndReturnsPublicEvent(
	t *testing.T,
) {
	es := store.New()
	w := newTestWriter(
		es,
		store.NewSnapshots(),
	)

	prev := order{Status: "Pending", Total: 50}
	next := order{Status: "Shipped", Total: 50}

	evt, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Shipped",
		0,
		prev,
		next,
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	if evt.AggregateID != "agg1" {
		t.Errorf(
			"AggregateID = %q, want agg1",
			evt.AggregateID,
		)
	}

	if evt.EventName != "Shipped" {
		t.Errorf(
			"EventName = %q, want Shipped",
			evt.EventName,
		)
	}

	if evt.Version != 1 {
		t.Errorf(
			"Version = %d, want 1 (first event)",
			evt.Version,
		)
	}

	if evt.Aggregate.Status != "Shipped" {
		t.Errorf(
			"Aggregate.Status = %q, want Shipped",
			evt.Aggregate.Status,
		)
	}

	if evt.PreviousAggregate.Status != "Pending" {
		t.Errorf(
			"PreviousAggregate.Status = %q, want Pending",
			evt.PreviousAggregate.Status,
		)
	}
}

func TestAppendTx_PatchRecordsOnlyDiff(
	t *testing.T,
) {
	es := store.New()
	w := newTestWriter(
		es,
		store.NewSnapshots(),
	)

	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Updated",
		0,
		order{Status: "Pending", Total: 50},
		order{Status: "Shipped", Total: 50},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	blobs, _ := es.ReadFrom(
		context.Background(),
		"events:agg1",
		0,
	)
	if len(blobs) == 0 {
		t.Fatal("no event blob written")
	}

	var stored esmodels.InternalEvent
	if err := json.Unmarshal(
		blobs[len(blobs)-1],
		&stored,
	); err != nil {
		t.Fatalf(
			"unmarshal stored event: %v",
			err,
		)
	}

	patchStr := string(stored.Patches)
	if patchStr == "[]" ||
		patchStr == "null" {
		t.Fatal("expected non-empty patch")
	}

	if bytes.Contains(
		stored.Patches,
		[]byte("Total"),
	) {
		t.Errorf(
			"patch contains Total field, want only Status: %s",
			patchStr,
		)
	}
}

func TestAppendTx_VersionIncrements(
	t *testing.T,
) {
	w := newTestWriter(
		store.New(),
		store.NewSnapshots(),
	)

	for i := int64(1); i <= 3; i++ {
		assertWriteVersion(
			t,
			w,
			i,
		)
	}
}

func assertWriteVersion(
	t *testing.T,
	w *Writer[order],
	i int64,
) {
	t.Helper()

	evt, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Event",
		i-1,
		order{},
		order{Status: "x"},
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

func TestAppendTx_AppendsAtExpectedVersionPlusOne(
	t *testing.T,
) {
	w := newTestWriter(
		store.New(),
		store.NewSnapshots(),
	)
	ctx := context.Background()

	evt, err := appendEvent(
		ctx,
		w,
		"agg1",
		"Next",
		7,
		order{},
		order{},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	if evt.Version != 8 {
		t.Errorf(
			"Version = %d, want 8 (expectedVersion 7 + 1)",
			evt.Version,
		)
	}
}

func TestSnapshot_WritesTheEventState(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	w := newTestWriter(
		es,
		ss,
	)

	_, err := appendSnapshotted(
		context.Background(),
		w,
		"agg1",
		"Created",
		0,
		order{},
		order{Status: "Active"},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	snapBlob, found, _ := ss.Get(
		context.Background(),
		"agg1",
	)
	if !found {
		t.Fatal("expected snapshot to be written")
	}

	var snap esmodels.SnapshotBlob[order]
	if err := json.Unmarshal(
		snapBlob,
		&snap,
	); err != nil {
		t.Fatalf(
			"unmarshal snapshot: %v",
			err,
		)
	}

	if snap.State.Status != "Active" {
		t.Errorf(
			"snapshot state.Status = %q, want Active",
			snap.State.Status,
		)
	}
}

func TestAppendTx_NeverWritesASnapshot(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	w := newTestWriter(
		store.New(),
		ss,
	)

	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Created",
		0,
		order{},
		order{},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	_, found, _ := ss.Get(
		context.Background(),
		"agg1",
	)
	if found {
		t.Error("expected no snapshot, got one")
	}
}

func TestAppendTx_EventStoreError_ReturnsError(
	t *testing.T,
) {
	w := newTestWriter(
		&mockstores.ErrStore{Err: storageErr},
		store.NewSnapshots(),
	)

	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		order{},
		order{},
	)
	if !errors.Is(
		err,
		storageErr,
	) {
		t.Errorf(
			"err = %v, want storageErr",
			err,
		)
	}
}

func TestAppendTx_EventStoreAppendError(
	t *testing.T,
) {
	es := store.New()
	es.SetError(
		"events:agg1",
		storageErr,
	)

	w := newTestWriter(
		es,
		store.NewSnapshots(),
	)

	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		order{},
		order{Status: "x"},
	)
	if !errors.Is(
		err,
		storageErr,
	) {
		t.Errorf(
			"err = %v, want storageErr",
			err,
		)
	}
}

func TestAppendTx_SnapshotStoreError_ReturnsError(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	ss.SetError(
		"agg1",
		storageErr,
	)
	w := newTestWriter(
		store.New(),
		ss,
	)

	_, err := appendSnapshotted(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		order{},
		order{},
	)
	if !errors.Is(
		err,
		storageErr,
	) {
		t.Errorf(
			"err = %v, want storageErr",
			err,
		)
	}
}

func TestAppendTx_MarshalPreviousStateError(
	t *testing.T,
) {
	w := New[mocks.ErrMarshal](
		store.New(),
		store.NewSnapshots(),
		1,
	)

	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		mocks.ErrMarshal{},
		mocks.ErrMarshal{},
	)
	if err == nil {
		t.Fatal("expected marshal error on previousState")
	}
}

func TestAppendTx_MarshalNewStateError(
	t *testing.T,
) {
	w := New[*mocks.CountedMarshal](
		store.New(),
		store.NewSnapshots(),
		1,
	)

	cm := &mocks.CountedMarshal{FailAt: 2}
	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		cm,
		cm,
	)
	if err == nil {
		t.Fatal("expected marshal error on newState")
	}
}

func TestSnapshot_MarshalStateError(
	t *testing.T,
) {
	w := New[*mocks.CountedMarshal](
		store.New(),
		store.NewSnapshots(),
		1,
	)

	cm := &mocks.CountedMarshal{FailAt: 3}
	_, err := appendSnapshotted(
		context.Background(),
		w,
		"agg1",
		"Evt",
		0,
		cm,
		cm,
	)
	if err == nil {
		t.Fatal("expected marshal error on newState before writeSnapshotFromBytes")
	}
}

func TestWriter_EventStore_ReturnsBacking(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	w := New[order](
		es,
		ss,
		1,
	)
	if w.EventStore() != es {
		t.Error("EventStore() did not return the event store passed to New")
	}
}

func TestWriter_SnapshotStore_ReturnsBacking(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	w := New[order](
		es,
		ss,
		1,
	)
	if w.SnapshotStore() != ss {
		t.Error("SnapshotStore() did not return the snapshot store passed to New")
	}
}

func TestWriter_SnapshotIsUpsertedNotAccumulated(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	w := New[order](
		events,
		snaps,
		1,
	)

	prev := order{}
	for i := range 20 {
		prev = writeSnapshotted(
			t,
			w,
			prev,
			i,
		)
	}

	blob, found, err := snaps.Get(
		ctx,
		"agg-1",
	)
	if err != nil ||
		!found {
		t.Fatalf(
			"get: found=%v err=%v",
			found,
			err,
		)
	}

	var snap esmodels.SnapshotBlob[order]
	if err := json.Unmarshal(
		blob,
		&snap,
	); err != nil {
		t.Fatalf(
			"unmarshal: %v",
			err,
		)
	}

	if snap.Version != 20 {
		t.Errorf(
			"snapshot version = %d, want 20 — Put must hold only the newest",
			snap.Version,
		)
	}

	if snap.State.Total != 19 {
		t.Errorf(
			"Total = %v, want 19",
			snap.State.Total,
		)
	}
}

func writeSnapshotted(
	t *testing.T,
	w *Writer[order],
	prev order,
	i int,
) order {
	t.Helper()

	next := order{ID: "agg-1", Total: float64(i), Status: "Pending"}
	_, err := appendSnapshotted(
		context.Background(),
		w,
		"agg-1",
		"OrderUpdated",
		int64(i),
		prev,
		next,
	)
	if err != nil {
		t.Fatalf(
			"write %d: %v",
			i,
			err,
		)
	}

	return next
}

type writeFunc func(
	w *Writer[order],
	prev order,
	next order,
) error

func BenchmarkWrite(
	b *testing.B,
) {
	b.Run(
		"no_snapshot",
		benchmarkPlainWrite,
	)
	b.Run(
		"with_snapshot",
		benchmarkSnapshottedWrite,
	)
}

func benchmarkPlainWrite(
	b *testing.B,
) {
	benchmarkWrite(
		b,
		plainWrite,
	)
}

func benchmarkSnapshottedWrite(
	b *testing.B,
) {
	benchmarkWrite(
		b,
		snapshottedWrite,
	)
}

func plainWrite(
	w *Writer[order],
	prev order,
	next order,
) error {
	_, err := appendEvent(
		context.Background(),
		w,
		"agg1",
		"Updated",
		0,
		prev,
		next,
	)
	return err
}

func snapshottedWrite(
	w *Writer[order],
	prev order,
	next order,
) error {
	_, err := appendSnapshotted(
		context.Background(),
		w,
		"agg1",
		"Updated",
		0,
		prev,
		next,
	)
	return err
}

func benchmarkWrite(
	b *testing.B,
	write writeFunc,
) {
	prevState := order{Status: "Pending", Total: 50}
	nextState := order{Status: "Shipped", Total: 50}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		w := newTestWriter(
			store.New(),
			store.NewSnapshots(),
		)
		b.StartTimer()
		mustWrite(
			b,
			write,
			w,
			prevState,
			nextState,
		)
	}
}

func mustWrite(
	b *testing.B,
	write writeFunc,
	w *Writer[order],
	prev order,
	next order,
) {
	b.Helper()

	err := write(
		w,
		prev,
		next,
	)
	if err != nil {
		b.Fatal(err)
	}
}

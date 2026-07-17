package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

// --- Helpers ---

func newTestWriter(es asynxmd.Store, ss asynxmd.SnapshotStore) *Writer[order] {
	return New[order](es, ss, 1)
}

// --- Write ---

func TestWrite_AppendsEventAndReturnsPublicEvent(t *testing.T) {
	es := store.New()
	w := newTestWriter(es, store.NewSnapshots())

	prev := order{Status: "Pending", Total: 50}
	next := order{Status: "Shipped", Total: 50}

	evt, err := w.Write(context.Background(), "agg1", "Shipped", 0, prev, next, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if evt.AggregateID != "agg1" {
		t.Errorf("AggregateID = %q, want agg1", evt.AggregateID)
	}
	if evt.EventName != "Shipped" {
		t.Errorf("EventName = %q, want Shipped", evt.EventName)
	}
	if evt.Version != 1 {
		t.Errorf("Version = %d, want 1 (first event)", evt.Version)
	}
	if evt.Aggregate.Status != "Shipped" {
		t.Errorf("Aggregate.Status = %q, want Shipped", evt.Aggregate.Status)
	}
	if evt.PreviousAggregate.Status != "Pending" {
		t.Errorf("PreviousAggregate.Status = %q, want Pending", evt.PreviousAggregate.Status)
	}
}

func TestWrite_PatchRecordsOnlyDiff(t *testing.T) {
	es := store.New()
	w := newTestWriter(es, store.NewSnapshots())

	_, err := w.Write(context.Background(), "agg1", "Updated", 0,
		order{Status: "Pending", Total: 50},
		order{Status: "Shipped", Total: 50},
		false,
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	blobs, _ := es.ReadFrom(context.Background(), "events:agg1", 0)
	if len(blobs) == 0 {
		t.Fatal("no event blob written")
	}
	var stored esmodels.InternalEvent
	if err := json.Unmarshal(blobs[len(blobs)-1], &stored); err != nil {
		t.Fatalf("unmarshal stored event: %v", err)
	}

	patchStr := string(stored.Patches)
	if patchStr == "[]" || patchStr == "null" {
		t.Fatal("expected non-empty patch")
	}
	if bytes.Contains(stored.Patches, []byte("Total")) {
		t.Errorf("patch contains Total field, want only Status: %s", patchStr)
	}
}

func TestWrite_VersionIncrements(t *testing.T) {
	es := store.New()
	w := newTestWriter(es, store.NewSnapshots())
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		evt, err := w.Write(ctx, "agg1", "Event", i-1, order{}, order{Status: "x"}, false)
		if err != nil {
			t.Fatalf("Write #%d: %v", i, err)
		}
		if evt.Version != i {
			t.Errorf("Write #%d: Version = %d, want %d", i, evt.Version, i)
		}
	}
}

func TestWrite_AppendsAtExpectedVersionPlusOne(t *testing.T) {
	w := newTestWriter(store.New(), store.NewSnapshots())
	ctx := context.Background()

	// The writer no longer derives the version from storage; it appends at
	// expectedVersion+1. Deriving the loaded version is the reader's job.
	evt, err := w.Write(ctx, "agg1", "Next", 7, order{}, order{}, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if evt.Version != 8 {
		t.Errorf("Version = %d, want 8 (expectedVersion 7 + 1)", evt.Version)
	}
}

func TestWrite_WritesSnapshot_WhenRequested(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	w := newTestWriter(es, ss)

	_, err := w.Write(context.Background(), "agg1", "Created", 0, order{}, order{Status: "Active"}, true)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapBlob, found, _ := ss.Get(context.Background(), "agg1")
	if !found {
		t.Fatal("expected snapshot to be written")
	}

	var snap esmodels.SnapshotBlob[order]
	if err := json.Unmarshal(snapBlob, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap.State.Status != "Active" {
		t.Errorf("snapshot state.Status = %q, want Active", snap.State.Status)
	}
}

func TestWrite_NoSnapshot_WhenNotRequested(t *testing.T) {
	ss := store.NewSnapshots()
	w := newTestWriter(store.New(), ss)

	_, err := w.Write(context.Background(), "agg1", "Created", 0, order{}, order{}, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	_, found, _ := ss.Get(context.Background(), "agg1")
	if found {
		t.Error("expected no snapshot, got one")
	}
}

func TestWrite_EventStoreError_ReturnsError(t *testing.T) {
	w := newTestWriter(&mocks.ErrStore{Err: storageErr}, store.NewSnapshots())

	_, err := w.Write(context.Background(), "agg1", "Evt", 0, order{}, order{}, false)
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestWrite_EventStoreAppendError(t *testing.T) {
	es := store.New()
	es.SetError("events:agg1", storageErr)

	w := newTestWriter(es, store.NewSnapshots())

	_, err := w.Write(context.Background(), "agg1", "Evt", 0, order{}, order{Status: "x"}, false)
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestWrite_SnapshotStoreError_ReturnsError(t *testing.T) {
	ss := store.NewSnapshots()
	ss.SetError("agg1", storageErr)
	w := newTestWriter(store.New(), ss)

	_, err := w.Write(context.Background(), "agg1", "Evt", 0, order{}, order{}, true)
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestWrite_MarshalPreviousStateError(t *testing.T) {
	w := New[mocks.ErrMarshal](store.New(), store.NewSnapshots(), 1)

	_, err := w.Write(context.Background(), "agg1", "Evt", 0, mocks.ErrMarshal{}, mocks.ErrMarshal{}, false)
	if err == nil {
		t.Fatal("expected marshal error on previousState")
	}
}

func TestWrite_MarshalNewStateError(t *testing.T) {
	w := New[*mocks.CountedMarshal](store.New(), store.NewSnapshots(), 1)

	cm := &mocks.CountedMarshal{FailAt: 2}
	_, err := w.Write(context.Background(), "agg1", "Evt", 0, cm, cm, false)
	if err == nil {
		t.Fatal("expected marshal error on newState")
	}
}

func TestWriteSnapshot_MarshalStateError(t *testing.T) {
	// call 1+2: jsondiff.Compare marshals previousState and newState internally.
	// call 3: json.Marshal(SnapshotBlob[T]{State: newState}) inside writeSnapshot.
	w := New[*mocks.CountedMarshal](store.New(), store.NewSnapshots(), 1)

	cm := &mocks.CountedMarshal{FailAt: 3}
	_, err := w.Write(context.Background(), "agg1", "Evt", 0, cm, cm, true)
	if err == nil {
		t.Fatal("expected marshal error on newState before writeSnapshotFromBytes")
	}
}

func TestWriter_EventStore_ReturnsBacking(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	w := New[order](es, ss, 1)
	if w.EventStore() != es {
		t.Error("EventStore() did not return the event store passed to New")
	}
}

func TestWriter_SnapshotStore_ReturnsBacking(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	w := New[order](es, ss, 1)
	if w.SnapshotStore() != ss {
		t.Error("SnapshotStore() did not return the snapshot store passed to New")
	}
}

// --- SnapshotStore: upsert semantics, version recovery ---

func TestWriter_SnapshotIsUpsertedNotAccumulated(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	w := New[order](events, snaps, 1)

	prev := order{}
	for i := range 20 {
		next := order{ID: "agg-1", Total: float64(i), Status: "Pending"}
		// expectedVersion is the version the state was loaded at; iteration i
		// writes version i+1, so it validated against version i.
		if _, err := w.Write(ctx, "agg-1", "OrderUpdated", int64(i), prev, next, true); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		prev = next
	}

	blob, found, err := snaps.Get(ctx, "agg-1")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}

	var snap esmodels.SnapshotBlob[order]
	if err := json.Unmarshal(blob, &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if snap.Version != 20 {
		t.Errorf("snapshot version = %d, want 20 — Put must hold only the newest", snap.Version)
	}
	if snap.State.Total != 19 {
		t.Errorf("Total = %v, want 19", snap.State.Total)
	}
}

// failingSnapshots returns err from every operation. Used where store.SnapshotMemory
// cannot simulate a Get failure (SetError only schedules a one-shot Put failure).
type failingSnapshots struct{ err error }

func (f *failingSnapshots) Put(context.Context, string, int64, []byte) error { return f.err }
func (f *failingSnapshots) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, f.err
}
func (f *failingSnapshots) Delete(context.Context, string) error { return f.err }

package reader

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/eventstore/replayer"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"github.com/wI2L/jsondiff"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

// --- Helpers ---

func newTestReader(es asynxmd.Store, ss asynxmd.SnapshotStore) *Reader[order] {
	rep := replayer.New[order](es, make(map[int]asynxmd.Upcaster), 1, order{})
	return New[order](es, ss, rep, 1, order{}, nil)
}

func makeEventBlob(t *testing.T, id, eventName string, version int64, schemaVersion int, patches json.RawMessage) []byte {
	t.Helper()
	evt := esmodels.InternalEvent{
		ID:            id,
		EventName:     eventName,
		Version:       version,
		SchemaVersion: schemaVersion,
		OccurredAt:    time.Now().UTC(),
		Patches:       patches,
	}
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("makeEventBlob: %v", err)
	}
	return b
}

func makeSnapshotBlob(t *testing.T, version int64, schemaVersion int, state order) []byte {
	t.Helper()
	snap := esmodels.SnapshotBlob[order]{
		Version:       version,
		SchemaVersion: schemaVersion,
		State:         state,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("makeSnapshotBlob: %v", err)
	}
	return b
}

// --- Get — cold path ---

func TestGet_ColdPath_ReturnsHydratedState(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Updated", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	r := newTestReader(es, store.NewSnapshots())

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", got.Status)
	}
}

func TestGet_ColdPath_ErrNotFound_WhenNoEvents(t *testing.T) {
	r := newTestReader(store.New(), store.NewSnapshots())

	_, err := r.Get(context.Background(), "missing")
	if !errors.Is(err, asynxmd.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestGet_ColdPath_StorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.NewSnapshots())

	_, err := r.Get(context.Background(), "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestGet_ColdPath_DeserializeError(t *testing.T) {
	// CorruptBlobStore returns a non-empty corrupt blob slice, so coldPath
	// skips ErrNotFound and calls deserializeEvents which fails.
	r := newTestReader(&mocks.CorruptBlobStore{}, store.NewSnapshots())

	_, err := r.Get(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected deserialize error on corrupt event blobs")
	}
}

// --- Get — warm path ---

func TestGet_WarmPath_SnapshotPlusDelta(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(ctx, "agg1", 1, makeSnapshotBlob(t, 1, 1, order{ID: "1", Status: "Pending", Total: 50}))                                                     //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Shipped", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	r := newTestReader(es, ss)

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get warm path: %v", err)
	}
	if got.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", got.Status)
	}
	if got.Total != 50 {
		t.Errorf("Total = %v, want 50 (from snapshot)", got.Total)
	}
}

func TestGet_WarmPath_NoDelta_ReturnsSnapshot(t *testing.T) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(ctx, "agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

	r := newTestReader(store.New(), ss)

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", got.Status)
	}
}

func TestGet_WarmPath_CorruptSnapshot_FallsBackToCold(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	// Outer blob is completely invalid JSON.
	ss.Put(ctx, "agg1", 1, []byte(`not-valid-json`))                                                                                                    //nolint:errcheck
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck

	r := newTestReader(es, ss)

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get with corrupt snapshot: %v", err)
	}
	if got.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (cold path fallback)", got.Status)
	}
}

func TestGet_WarmPath_CorruptSnapshot_CallsOnCorruption(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	// state field is a JSON string — cannot unmarshal into order struct.
	ss.Put(ctx, "agg1", 1, []byte(`{"version":1,"schema_version":1,"state":"not an order object"}`)) //nolint:errcheck
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	var hookErr error
	rep := replayer.New[order](es, make(map[int]asynxmd.Upcaster), 1, order{})
	r := New[order](es, ss, rep, 1, order{}, func(err error) { hookErr = err })

	_, _ = r.Get(ctx, "agg1")
	if hookErr == nil {
		t.Error("expected onCorruption to be called with an error")
	}
}

func TestGet_WarmPath_CorruptStateInSnapshot_FallsBackToCold(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	// state field is a JSON string — cannot unmarshal into order struct,
	// so the single json.Unmarshal(&SnapshotBlob[T]) call fails → cold path.
	ss.Put(ctx, "agg1", 1, []byte(`{"version":1,"schema_version":1,"state":"not an order object"}`))                                                    //nolint:errcheck
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	r := newTestReader(es, ss)

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped (cold path fallback)", got.Status)
	}
}

func TestGet_WarmPath_DeltaDeserializeError(t *testing.T) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(ctx, "agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

	r := newTestReader(&mocks.CorruptBlobStore{}, ss)

	_, err := r.Get(ctx, "agg1")
	if err == nil {
		t.Fatal("expected deserialize error on corrupt delta blobs")
	}
}

func TestGet_WarmPath_DeltaStorageError(t *testing.T) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(ctx, "agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

	r := newTestReader(&mocks.ErrStore{Err: storageErr}, ss)

	_, err := r.Get(ctx, "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestGet_SnapshotStorageError(t *testing.T) {
	r := newTestReader(store.New(), &failingSnapshots{err: storageErr})

	_, err := r.Get(context.Background(), "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

// --- Get — auto-snapshot on upcasting (moved from replayer_test) ---

func TestGet_AutoSnapshotFail_ReturnsState(t *testing.T) {
	// This test verifies the fix: auto-snapshot failures should not prevent
	// returning valid state. The state is already durable in the event store.
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1,
		json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	// Make snapshot store fail on the auto-snapshot write.
	ss.SetError("agg1", storageErr)

	state, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: expected no error, got %v", err)
	}
	if state.Status != "Shipped" {
		t.Errorf("state.Status = %q, want Shipped (state from events)", state.Status)
	}
}

func TestGet_AutoSnapshot_WrittenAfterUpcasting(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	_, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	_, found, _ := ss.Get(ctx, "agg1")
	if !found {
		t.Fatal("expected 1 auto-snapshot written, got none")
	}
}

// --- Exists ---

func TestExists_ReturnsTrueWhenEventExists(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	r := newTestReader(es, store.NewSnapshots())

	ok, err := r.Exists(ctx, "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("Exists = false, want true")
	}
}

func TestExists_ReturnsFalseWhenNoEvents(t *testing.T) {
	r := newTestReader(store.New(), store.NewSnapshots())

	ok, err := r.Exists(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("Exists = true, want false")
	}
}

func TestExists_StorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.NewSnapshots())

	_, err := r.Exists(context.Background(), "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

// --- Preload ---

func TestPreload_NoError_WhenAggregateExists(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	r := newTestReader(es, store.NewSnapshots())

	if err := r.Preload(ctx, "agg1"); err != nil {
		t.Errorf("Preload: %v", err)
	}
}

func TestPreload_NoError_WhenAggregateNotFound(t *testing.T) {
	r := newTestReader(store.New(), store.NewSnapshots())

	if err := r.Preload(context.Background(), "missing"); err != nil {
		t.Errorf("Preload on missing aggregate: %v (want nil)", err)
	}
}

func TestPreload_PropagatesStorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.NewSnapshots())

	err := r.Preload(context.Background(), "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

// --- Auto-snapshot error handling ---

func TestGet_WarmPath_AutoSnapshotError_ReturnsStateNotError(t *testing.T) {
	// This test verifies the fix for auto-snapshot error handling.
	// When a snapshot write fails during upcasting in the warm path,
	// the state should still be returned (not an error) because the state
	// is already durable in the event store.
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	// Create a snapshot at version 1
	ss.Put(ctx, "agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Old"})) //nolint:errcheck

	// Create an event at version 2 that will trigger upcasting
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e1", "Updated", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"New"}]`))) //nolint:errcheck

	// Create a reader with upcasters to trigger DidUpcast
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	// Make snapshot store return an error on the next write (for auto-snapshot)
	ss.SetError("agg1", storageErr)

	// Get should still return the state even though auto-snapshot write fails
	state, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: expected no error, got %v", err)
	}
	if state.Status != "New" {
		t.Errorf("Status = %q, want New (correct state from events)", state.Status)
	}
}

func TestGet_ColdPath_AutoSnapshotError_ReturnsStateNotError(t *testing.T) {
	// Similar to warm path: auto-snapshot errors should not prevent returning valid state.
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	// Create events that trigger upcasting
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	// Make snapshot store return an error on write (for auto-snapshot)
	ss.SetError("agg1", storageErr)

	// Get should still return the state even though auto-snapshot write fails
	state, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: expected no error, got %v", err)
	}
	if state.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (correct state from events)", state.Status)
	}
}

func TestGet_ColdPath_HydrateError_ReturnsError(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck

	sentinel := errors.New("upcaster failed")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) { return nil, sentinel },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	_, err := r.Get(ctx, "agg1")
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel upcaster error, got %v", err)
	}
}

// --- SnapshotStore: read exactly once, cold path on miss/corruption ---

// writeEvent appends a single event for aggregateID directly to a
// models.Store, computing the RFC 6902 patch from the zero value to state.
func writeEvent(t *testing.T, s asynxmd.Store, id string, version int64, state order) {
	t.Helper()
	patch, err := jsondiff.Compare(order{}, state)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	patchJSON, err := json.Marshal(patch)
	if err != nil {
		t.Fatalf("marshal patch: %v", err)
	}
	evt := esmodels.InternalEvent{
		ID:            "evt-1",
		EventName:     "OrderUpdated",
		Version:       version,
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC(),
		Patches:       json.RawMessage(patchJSON),
	}
	blob, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if err := s.Append(context.Background(), "events:"+id, version, blob); err != nil {
		t.Fatalf("append: %v", err)
	}
}

func TestReader_ReadsSnapshotExactlyOnce(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := &mocks.CountingSnapshotStore{Inner: store.NewSnapshots()}
	rep := replayer.New[order](events, nil, 1, order{})
	r := New[order](events, snaps, rep, 1, order{}, nil)

	writeEvent(t, events, "agg-1", 1, order{ID: "agg-1", Total: 1, Status: "Pending"})
	blob := makeSnapshotBlob(t, 1, 1, order{ID: "agg-1", Total: 1, Status: "Pending"})
	if err := snaps.Put(ctx, "agg-1", 1, blob); err != nil {
		t.Fatalf("put: %v", err)
	}

	snaps.Reset()
	if _, err := r.Get(ctx, "agg-1"); err != nil {
		t.Fatalf("get: %v", err)
	}

	if snaps.GetCalls != 1 {
		t.Errorf("snapshot Get calls = %d, want exactly 1", snaps.GetCalls)
	}
	if snaps.GetBytes != len(blob) {
		t.Errorf("bytes read = %d, want %d (exactly one blob)", snaps.GetBytes, len(blob))
	}
}

func TestReader_MissingSnapshotTakesColdPath(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](events, nil, 1, order{})
	r := New[order](events, snaps, rep, 1, order{}, nil)

	writeEvent(t, events, "agg-1", 1, order{ID: "agg-1", Total: 42, Status: "Pending"})

	got, err := r.Get(ctx, "agg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Total != 42 {
		t.Errorf("Total = %v, want 42", got.Total)
	}
}

func TestReader_CorruptSnapshotFallsBackToColdPath(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](events, nil, 1, order{})

	var corruptionErr error
	r := New[order](events, snaps, rep, 1, order{}, func(err error) {
		corruptionErr = err
	})

	writeEvent(t, events, "agg-1", 1, order{ID: "agg-1", Total: 42, Status: "Pending"})
	if err := snaps.Put(ctx, "agg-1", 1, []byte("{not json")); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := r.Get(ctx, "agg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Total != 42 {
		t.Errorf("Total = %v, want 42 — corrupt snapshot must cold-replay", got.Total)
	}
	if corruptionErr == nil {
		t.Error("onCorruption was not called")
	}
}

func TestReader_SnapshotStoreErrorPropagates(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := &failingSnapshots{err: storageErr}
	rep := replayer.New[order](events, nil, 1, order{})
	r := New[order](events, snaps, rep, 1, order{}, nil)

	if _, err := r.Get(ctx, "agg-1"); err == nil {
		t.Fatal("err = nil, want storage error")
	}
}

func TestReader_StaleSnapshotStillHydratesCorrectState(t *testing.T) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](events, nil, 1, order{})
	r := New[order](events, snaps, rep, 1, order{}, nil)

	// Write events 1..5, each advancing Total and the last advancing Status.
	writeEvent(t, events, "agg-1", 1, order{ID: "agg-1", Total: 1, Status: "Pending"})
	writeEvent(t, events, "agg-1", 2, order{ID: "agg-1", Total: 2, Status: "Pending"})
	writeEvent(t, events, "agg-1", 3, order{ID: "agg-1", Total: 3, Status: "Pending"})
	writeEvent(t, events, "agg-1", 4, order{ID: "agg-1", Total: 4, Status: "Pending"})
	writeEvent(t, events, "agg-1", 5, order{ID: "agg-1", Total: 5, Status: "Shipped"})

	// Simulate last-write-wins landing a STALE snapshot (v2) after newer
	// events already exist — impossible under the old append-only Store
	// design (reading all and taking the last always got the highest
	// version), but explicitly tolerated by models.SnapshotStore.Put.
	stale := makeSnapshotBlob(t, 2, 1, order{ID: "agg-1", Total: 2, Status: "Pending"})
	if err := snaps.Put(ctx, "agg-1", 2, stale); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := r.Get(ctx, "agg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// The reader must replay events 3, 4, 5 on top of the stale v2 snapshot
	// state to reach the CURRENT state, not return the stale v2 state.
	want := order{ID: "agg-1", Total: 5, Status: "Shipped"}
	if got != want {
		t.Errorf("Get with stale snapshot = %+v, want %+v (current state, not stale v2 snapshot)", got, want)
	}
}

// failingSnapshots returns err from Get.
type failingSnapshots struct{ err error }

func (f *failingSnapshots) Put(context.Context, string, int64, []byte) error { return f.err }
func (f *failingSnapshots) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, f.err
}
func (f *failingSnapshots) Delete(context.Context, string) error { return f.err }

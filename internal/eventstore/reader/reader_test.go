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
	"github.com/char2cs/asynx/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

// --- Helpers ---

func newTestReader(es, ss asynxmd.Store) *Reader[order] {
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

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`)))  //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Updated", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	r := newTestReader(es, store.New())

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", got.Status)
	}
}

func TestGet_ColdPath_ErrNotFound_WhenNoEvents(t *testing.T) {
	r := newTestReader(store.New(), store.New())

	_, err := r.Get(context.Background(), "missing")
	if !errors.Is(err, asynxmd.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestGet_ColdPath_StorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.New())

	_, err := r.Get(context.Background(), "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestGet_ColdPath_DeserializeError(t *testing.T) {
	// CorruptBlobStore returns a non-empty corrupt blob slice, so coldPath
	// skips ErrNotFound and calls deserializeEvents which fails.
	r := newTestReader(&mocks.CorruptBlobStore{}, store.New())

	_, err := r.Get(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected deserialize error on corrupt event blobs")
	}
}

// --- Get — warm path ---

func TestGet_WarmPath_SnapshotPlusDelta(t *testing.T) {
	es := store.New()
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, makeSnapshotBlob(t, 1, 1, order{ID: "1", Status: "Pending", Total: 50})) //nolint:errcheck
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
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

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
	ss := store.New()
	ctx := context.Background()

	// Outer blob is completely invalid JSON.
	ss.Append(ctx, "snapshots:agg1", 1, []byte(`not-valid-json`)) //nolint:errcheck
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
	ss := store.New()
	ctx := context.Background()

	// state field is a JSON string — cannot unmarshal into order struct.
	ss.Append(ctx, "snapshots:agg1", 1, []byte(`{"version":1,"schema_version":1,"state":"not an order object"}`)) //nolint:errcheck
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`)))             //nolint:errcheck

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
	ss := store.New()
	ctx := context.Background()

	// state field is a JSON string — cannot unmarshal into order struct,
	// so the single json.Unmarshal(&SnapshotBlob[T]) call fails → cold path.
	ss.Append(ctx, "snapshots:agg1", 1, []byte(`{"version":1,"schema_version":1,"state":"not an order object"}`)) //nolint:errcheck
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
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

	r := newTestReader(&mocks.CorruptBlobStore{}, ss)

	_, err := r.Get(ctx, "agg1")
	if err == nil {
		t.Fatal("expected deserialize error on corrupt delta blobs")
	}
}

func TestGet_WarmPath_DeltaStorageError(t *testing.T) {
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Pending"})) //nolint:errcheck

	r := newTestReader(&mocks.ErrStore{Err: storageErr}, ss)

	_, err := r.Get(ctx, "agg1")
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestGet_SnapshotStorageError(t *testing.T) {
	r := newTestReader(store.New(), &mocks.ErrStore{Err: storageErr})

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
	ss := store.New()
	ctx := context.Background()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1,
		json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	// Make snapshot store fail on the auto-snapshot write.
	ss.SetError("snapshots:agg1", storageErr)

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
	ss := store.New()
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

	snapBlobs, _ := ss.ReadFrom(ctx, "snapshots:agg1", 0)
	if len(snapBlobs) != 1 {
		t.Fatalf("expected 1 auto-snapshot written, got %d", len(snapBlobs))
	}
}

// --- Exists ---

func TestExists_ReturnsTrueWhenEventExists(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	r := newTestReader(es, store.New())

	ok, err := r.Exists(ctx, "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Error("Exists = false, want true")
	}
}

func TestExists_ReturnsFalseWhenNoEvents(t *testing.T) {
	r := newTestReader(store.New(), store.New())

	ok, err := r.Exists(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Error("Exists = true, want false")
	}
}

func TestExists_StorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.New())

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

	r := newTestReader(es, store.New())

	if err := r.Preload(ctx, "agg1"); err != nil {
		t.Errorf("Preload: %v", err)
	}
}

func TestPreload_NoError_WhenAggregateNotFound(t *testing.T) {
	r := newTestReader(store.New(), store.New())

	if err := r.Preload(context.Background(), "missing"); err != nil {
		t.Errorf("Preload on missing aggregate: %v (want nil)", err)
	}
}

func TestPreload_PropagatesStorageError(t *testing.T) {
	r := newTestReader(&mocks.ErrStore{Err: storageErr}, store.New())

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
	ss := store.New()
	ctx := context.Background()

	// Create a snapshot at version 1
	ss.Append(ctx, "snapshots:agg1", 1, makeSnapshotBlob(t, 1, 1, order{Status: "Old"})) //nolint:errcheck

	// Create an event at version 2 that will trigger upcasting
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e1", "Updated", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"New"}]`))) //nolint:errcheck

	// Create a reader with upcasters to trigger DidUpcast
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	// Make snapshot store return an error on the next write (for auto-snapshot)
	ss.SetError("snapshots:agg1", storageErr)

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
	ss := store.New()
	ctx := context.Background()

	// Create events that trigger upcasting
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	rep := replayer.New[order](es, upcasters, 2, order{})
	r := New[order](es, ss, rep, 2, order{}, nil)

	// Make snapshot store return an error on write (for auto-snapshot)
	ss.SetError("snapshots:agg1", storageErr)

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
	ss := store.New()
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

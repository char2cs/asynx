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
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

// --- Helpers ---

func newTestReader(es, ss asynxmd.Store) *Reader[order] {
	r := &replayer.Replayer[order]{
		EventStore:           es,
		SnapshotStore:        ss,
		Upcasters:            make(map[int]asynxmd.Upcaster),
		CurrentSchemaVersion: 1,
	}
	return &Reader[order]{
		EventStore:    es,
		SnapshotStore: ss,
		Replayer:      r,
	}
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
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("makeSnapshotBlob marshal state: %v", err)
	}
	snap := esmodels.SnapshotBlob{
		Version:       version,
		SchemaVersion: schemaVersion,
		State:         stateBytes,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("makeSnapshotBlob: %v", err)
	}
	return b
}

// makeRawSnapshotBlob creates a SnapshotBlob whose State field contains raw
// arbitrary bytes. The outer SnapshotBlob marshals successfully, but
// json.Unmarshal(snap.State, &T) will fail if rawState is not valid JSON
// for T. Used to exercise reader.go:52–54.
func makeRawSnapshotBlob(t *testing.T, version int64, schemaVersion int, rawState []byte) []byte {
	t.Helper()
	snap := esmodels.SnapshotBlob{
		Version:       version,
		SchemaVersion: schemaVersion,
		State:         rawState,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("makeRawSnapshotBlob: %v", err)
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
	// skips ErrNotFound and calls deserializeEvents which fails (reader.go:84–86).
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

func TestGet_WarmPath_CorruptStateInSnapshot_FallsBackToCold(t *testing.T) {
	es := store.New()
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, []byte(`{"Version":1,"SchemaVersion":1,"State":"not-base64-json"}`)) //nolint:errcheck
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

func TestGet_WarmPath_StateUnmarshalError_FallsBackToCold(t *testing.T) {
	es := store.New()
	ss := store.New()
	ctx := context.Background()

	ss.Append(ctx, "snapshots:agg1", 1, makeRawSnapshotBlob(t, 1, 1, []byte("not valid json"))) //nolint:errcheck
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`))) //nolint:errcheck

	r := newTestReader(es, ss)

	got, err := r.Get(ctx, "agg1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (cold path fallback)", got.Status)
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

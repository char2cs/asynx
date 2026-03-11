package eventstore

import (
	"context"
	"encoding/json"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- Reader.Get: warm path ---

func TestReaderGet_WarmPath(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Snapshot at version 1 (Status=Pending).
	snapBlob := makeSnapshotBlob(t, 1, 1, order{ID: "1", Status: "Pending", Total: 100})
	ss.Append(context.Background(), "snapshots:agg1", 1, snapBlob) //nolint:errcheck

	// Delta event at version 2: ship the order.
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)
	es.Append(context.Background(), "events:agg1", 2, makeInternalEventBlob(t, "e2", "Shipped", 2, 1, patch)) //nolint:errcheck

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	state, err := reader.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get (warm path): %v", err)
	}
	if state.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", state.Status)
	}
	if state.Total != 100 {
		t.Errorf("Total = %v, want 100", state.Total)
	}
}

func TestReaderGet_WarmPath_NoDeltas(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	snapBlob := makeSnapshotBlob(t, 3, 1, order{ID: "1", Status: "Complete", Total: 200})
	ss.Append(context.Background(), "snapshots:agg1", 3, snapBlob) //nolint:errcheck

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	state, err := reader.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get (warm path, no deltas): %v", err)
	}
	if state.Status != "Complete" {
		t.Errorf("Status = %q, want Complete", state.Status)
	}
}

// --- Reader.Get: cold path ---

func TestReaderGet_ColdPath(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// No snapshot — full cold replay.
	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	state, err := reader.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get (cold path): %v", err)
	}
	if state.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", state.Status)
	}
}

func TestReaderGet_NotFound(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	_, err := reader.Get(context.Background(), "no-such-aggregate")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- Reader.Get: snapshot corruption fallback ---

func TestReaderGet_CorruptSnapshotBlob_FallsBackToCold(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Store a corrupt (non-JSON) snapshot blob.
	ss.Append(context.Background(), "snapshots:agg1", 1, []byte("not-json")) //nolint:errcheck

	// But event store has valid events.
	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	state, err := reader.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get (corrupt snapshot): %v", err)
	}
	// Should have fallen back to cold path successfully.
	if state.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped (cold path result)", state.Status)
	}
}

func TestReaderGet_CorruptSnapshotState_FallsBackToCold(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Valid snapshotBlob but with corrupted State bytes.
	snap := snapshotBlob{Version: 1, SchemaVersion: 1, State: []byte("not-valid-json-state")}
	snapJSON, _ := json.Marshal(snap)
	ss.Append(context.Background(), "snapshots:agg1", 1, snapJSON) //nolint:errcheck

	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	state, err := reader.Get(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Get (corrupt snapshot state): %v", err)
	}
	if state.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped (cold path result)", state.Status)
	}
}

// --- Reader.Get: storage errors ---

func TestReaderGet_SnapshotStorageError_Propagates(t *testing.T) {
	es := newMemStore()
	ss := &errStore{err: storageErr}

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	_, err := reader.Get(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected storage error from snapshot store")
	}
}

func TestReaderGet_EventStorageError_Propagates(t *testing.T) {
	es := &errStore{err: storageErr}
	ss := newMemStore() // No snapshot, so we'll try cold path.

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	_, err := reader.Get(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected storage error from event store")
	}
}

// --- Reader.Exists ---

func TestReaderExists_True(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	exists, err := reader.Exists(context.Background(), "agg1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("expected Exists to return true")
	}
}

func TestReaderExists_False(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	exists, err := reader.Exists(context.Background(), "no-such")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("expected Exists to return false for unknown aggregate")
	}
}

func TestReaderExists_StorageError(t *testing.T) {
	es := &errStore{err: storageErr}
	ss := newMemStore()

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	_, err := reader.Exists(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected storage error from Exists")
	}
}

// --- Reader.Preload ---

func TestReaderPreload_Success(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	if err := reader.Preload(context.Background(), "agg1"); err != nil {
		t.Errorf("Preload: %v", err)
	}
}

func TestReaderPreload_NotFoundIsOk(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	// Preload on non-existent aggregate must not return error.
	if err := reader.Preload(context.Background(), "unknown"); err != nil {
		t.Errorf("Preload for unknown aggregate returned error: %v", err)
	}
}

func TestReaderPreload_StorageError_Propagates(t *testing.T) {
	es := &errStore{err: storageErr}
	ss := newMemStore()

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	if err := reader.Preload(context.Background(), "agg1"); err == nil {
		t.Error("expected storage error from Preload")
	}
}

// --- deserializeEvents: corrupt event blob ---

func TestDeserializeEvents_CorruptBlob(t *testing.T) {
	_, err := deserializeEvents([][]byte{[]byte("not-json")})
	if err == nil {
		t.Fatal("expected error on corrupt event blob")
	}
}

// --- Reader.Get: corrupt delta events ---

func TestReaderGet_CorruptDeltaEvent_Propagates(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Snapshot at version 1.
	snapBlob := makeSnapshotBlob(t, 1, 1, order{ID: "1", Status: "Pending"})
	ss.Append(context.Background(), "snapshots:agg1", 1, snapBlob) //nolint:errcheck

	// Corrupt event at version 2.
	es.Append(context.Background(), "events:agg1", 2, []byte("bad-json")) //nolint:errcheck

	r := newTestReplayer(es, ss, nil, 1)
	reader := newTestReader(es, ss, r)

	_, err := reader.Get(context.Background(), "agg1")
	if err == nil {
		t.Fatal("expected error on corrupt delta event")
	}
}

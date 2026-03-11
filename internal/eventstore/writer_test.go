package eventstore

import (
	"context"
	"encoding/json"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

func TestWriterWrite_BasicDiff(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	prev := order{ID: "1", Status: "Pending", Total: 100}
	next := order{ID: "1", Status: "Shipped", Total: 100}

	evt, err := w.Write(context.Background(), "1", "OrderShipped", prev, next, 1, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if evt.Aggregate.Status != "Shipped" {
		t.Errorf("Aggregate.Status = %q, want Shipped", evt.Aggregate.Status)
	}
	if evt.PreviousAggregate.Status != "Pending" {
		t.Errorf("PreviousAggregate.Status = %q, want Pending", evt.PreviousAggregate.Status)
	}
	if evt.Version != 1 {
		t.Errorf("Version = %d, want 1", evt.Version)
	}
	if evt.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", evt.SchemaVersion)
	}
	if evt.EventName != "OrderShipped" {
		t.Errorf("EventName = %q, want OrderShipped", evt.EventName)
	}
	if evt.AggregateID != "1" {
		t.Errorf("AggregateID = %q, want 1", evt.AggregateID)
	}

	// Event must be in the event store.
	blobs, _ := es.ReadFrom(context.Background(), "events:1", 1)
	if len(blobs) != 1 {
		t.Fatalf("expected 1 event blob, got %d", len(blobs))
	}

	var stored internalEvent
	if err := json.Unmarshal(blobs[0], &stored); err != nil {
		t.Fatalf("unmarshal stored event: %v", err)
	}
	if stored.EventName != "OrderShipped" {
		t.Errorf("stored EventName = %q, want OrderShipped", stored.EventName)
	}
}

func TestWriterWrite_EmptyDiff(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	state := order{ID: "1", Status: "Pending", Total: 100}

	// Same old and new — empty patch.
	evt, err := w.Write(context.Background(), "1", "Noop", state, state, 1, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	if evt.EventName != "Noop" {
		t.Errorf("EventName = %q, want Noop", evt.EventName)
	}

	// Event must still be stored even with empty patches.
	blobs, _ := es.ReadFrom(context.Background(), "events:1", 1)
	if len(blobs) != 1 {
		t.Fatalf("expected 1 event blob for empty diff, got %d", len(blobs))
	}
}

func TestWriterWrite_WithSnapshot(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	prev := order{ID: "1", Status: "Pending", Total: 50}
	next := order{ID: "1", Status: "Checked Out", Total: 50}

	_, err := w.Write(context.Background(), "1", "CheckedOut", prev, next, 1, true)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:1", 0)
	if len(snapBlobs) != 1 {
		t.Fatalf("expected 1 snapshot blob, got %d", len(snapBlobs))
	}

	var snap snapshotBlob
	if err := json.Unmarshal(snapBlobs[0], &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap.Version != 1 {
		t.Errorf("snap.Version = %d, want 1", snap.Version)
	}

	var snapState order
	json.Unmarshal(snap.State, &snapState)
	if snapState.Status != "Checked Out" {
		t.Errorf("snap state Status = %q, want Checked Out", snapState.Status)
	}
}

func TestWriterWrite_NilPreviousState(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	var prev order // zero value (nil aggregate)
	next := order{ID: "1", Status: "Pending", Total: 100}

	_, err := w.Write(context.Background(), "1", "OrderCreated", prev, next, 1, false)
	if err != nil {
		t.Fatalf("Write with zero-value previous: %v", err)
	}
}

func TestWriterWrite_AppendError(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	es.setError("events:1", storageErr)

	_, err := w.Write(context.Background(), "1", "OrderCreated", order{}, order{ID: "1"}, 1, false)
	if err == nil {
		t.Fatal("expected error on Append failure, got nil")
	}
}

func TestWriterWrite_VersionConflict(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	state := order{ID: "1", Status: "Pending"}

	// First write succeeds.
	_, err := w.Write(context.Background(), "1", "OrderCreated", order{}, state, 1, false)
	if err != nil {
		t.Fatalf("first Write: %v", err)
	}

	// Second write at same version must fail.
	_, err = w.Write(context.Background(), "1", "OrderCreated", order{}, state, 1, false)
	if err != asynxmd.ErrPipelineFailed {
		t.Errorf("expected ErrPipelineFailed on version conflict, got %v", err)
	}
}

func TestWriterWrite_SnapshotError(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	// Event will succeed, snapshot will fail.
	ss.setError("snapshots:1", storageErr)

	_, err := w.Write(context.Background(), "1", "CheckedOut", order{}, order{ID: "1"}, 1, true)
	if err == nil {
		t.Fatal("expected error on snapshot write failure")
	}

	// But the event itself should be stored.
	blobs, _ := es.ReadFrom(context.Background(), "events:1", 1)
	if len(blobs) != 1 {
		t.Error("event should still be stored even when snapshot write fails")
	}
}

func TestWriterWriteSnapshot_SerializationError(t *testing.T) {
	// writeSnapshot is exercised indirectly; test path where snapshot store errors.
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 1)

	// The snapshot store will reject the write.
	ss.setError("snapshots:agg1", storageErr)

	_, err := w.Write(context.Background(), "agg1", "CheckedOut", order{}, order{ID: "agg1", Status: "Complete"}, 1, true)
	if err == nil {
		t.Fatal("expected error when snapshot store rejects write")
	}
}

func TestWriterWrite_SchemaVersionStamped(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	w := newTestWriter(es, ss, 3)

	evt, err := w.Write(context.Background(), "1", "Created", order{}, order{ID: "1"}, 1, false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if evt.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", evt.SchemaVersion)
	}

	blobs, _ := es.ReadFrom(context.Background(), "events:1", 1)
	var stored internalEvent
	json.Unmarshal(blobs[0], &stored)
	if stored.SchemaVersion != 3 {
		t.Errorf("stored SchemaVersion = %d, want 3", stored.SchemaVersion)
	}
}

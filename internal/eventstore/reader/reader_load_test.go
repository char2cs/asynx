package reader

import (
	"context"
	"encoding/json"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

// Load returns the state together with the version it was built at. That
// version is what makes optimistic-concurrency writes correct, so it gets its
// own coverage across the new/cold/warm paths.

func TestLoad_NewAggregate_ReturnsNotFoundAndZero(t *testing.T) {
	r := newTestReader(store.New(), store.NewSnapshots())

	_, version, err := r.Load(context.Background(), "ghost")
	if err != asynxmd.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if version != 0 {
		t.Errorf("version = %d, want 0", version)
	}
}

func TestLoad_ColdPath_ReturnsLastEventVersion(t *testing.T) {
	es := store.New()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, patch)) //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Updated", 2, 1, patch)) //nolint:errcheck

	r := newTestReader(es, store.NewSnapshots())

	_, version, err := r.Load(ctx, "agg1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2 (last event)", version)
	}
}

func TestLoad_WarmPath_ContinuesFromSnapshot(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	ss.Put(ctx, "agg1", 5, makeSnapshotBlob(t, 5, 1, order{}))                 //nolint:errcheck
	es.Append(ctx, "events:agg1", 6, makeEventBlob(t, "e6", "U", 6, 1, patch)) //nolint:errcheck
	es.Append(ctx, "events:agg1", 7, makeEventBlob(t, "e7", "U", 7, 1, patch)) //nolint:errcheck

	r := newTestReader(es, ss)

	_, version, err := r.Load(ctx, "agg1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 7 {
		t.Errorf("version = %d, want 7 (last delta after snapshot)", version)
	}
}

func TestLoad_WarmPath_SnapshotWithNoDeltas_ReturnsSnapshotVersion(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	ss.Put(ctx, "agg1", 5, makeSnapshotBlob(t, 5, 1, order{Status: "Snapped"})) //nolint:errcheck

	r := newTestReader(es, ss)

	_, version, err := r.Load(ctx, "agg1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 5 {
		t.Errorf("version = %d, want 5 (snapshot version)", version)
	}
}

// TestLoad_CorruptSnapshot_ReturnsColdPathVersion preserves the concern the
// writer used to own before optimistic concurrency moved version derivation
// here: a snapshot that cannot be deserialized must not produce a wrong
// version. Load falls back to a full replay and reports the last event's
// version, so the expected version handed to Write stays correct.
func TestLoad_CorruptSnapshot_ReturnsColdPathVersion(t *testing.T) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, patch)) //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Updated", 2, 1, patch)) //nolint:errcheck
	ss.Put(ctx, "agg1", 1, []byte("{not json"))                                      //nolint:errcheck

	r := newTestReader(es, ss)

	_, version, err := r.Load(ctx, "agg1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2 — a corrupt snapshot must cold-replay to the last event version", version)
	}
}

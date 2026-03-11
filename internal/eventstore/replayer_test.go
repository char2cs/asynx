package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- Replayer.hydrate tests ---

func TestReplayerHydrate_BasicPatchApplication(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	seed := order{ID: "1", Status: "Pending", Total: 50}

	// Build a patch that sets Status to "Shipped".
	patches := json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)
	events := []internalEvent{
		{ID: "e1", EventName: "Shipped", Version: 1, SchemaVersion: 1, Patches: patches},
	}

	result, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if result.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", result.Status)
	}
	if result.Total != 50 {
		t.Errorf("Total = %v, want 50", result.Total)
	}
}

func TestReplayerHydrate_MultiplePatches(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	seed := order{ID: "1", Status: "Pending", Total: 0}

	events := []internalEvent{
		{
			ID: "e1", Version: 1, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/Total","value":100}]`),
		},
		{
			ID: "e2", Version: 2, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		},
	}

	result, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	if result.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", result.Status)
	}
	if result.Total != 100 {
		t.Errorf("Total = %v, want 100", result.Total)
	}
}

func TestReplayerHydrate_EmptyEvents(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	seed := order{ID: "1", Status: "Pending"}

	result, err := r.hydrate(context.Background(), "agg1", seed, nil)
	if err != nil {
		t.Fatalf("hydrate with empty events: %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (seed unchanged)", result.Status)
	}
}

func TestReplayerHydrate_EmptyPatches(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	seed := order{ID: "1", Status: "Pending"}
	events := []internalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	result, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("hydrate with empty patches: %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (no-op patch)", result.Status)
	}
}

func TestReplayerHydrate_WithUpcasting_WritesAutoSnapshot(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Upcaster renames path /Status → /OrderStatus in patches.
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, patches []byte) ([]byte, error) {
			// The patch references "/Status"; rename to "/Status" (identity for test).
			return patches, nil
		},
	}

	r := newTestReplayer(es, ss, upcasters, 2) // currentSchemaVersion=2

	seed := order{ID: "1", Status: "Pending"}
	events := []internalEvent{
		{
			ID: "e1", Version: 1, SchemaVersion: 1, // needs upcasting (1 → 2)
			Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		},
	}

	_, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("hydrate with upcasting: %v", err)
	}

	// Auto-snapshot should have been written to the snapshot store.
	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snapBlobs))
	}
}

func TestReplayerHydrate_NoAutoSnapshot_WhenNoUpcasting(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	seed := order{ID: "1", Status: "Pending"}
	events := []internalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("hydrate: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 0 {
		t.Error("expected no auto-snapshot when no upcasting occurred")
	}
}

func TestReplayerHydrate_AutoSnapshotFail_ReturnsStateAndError(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	ss.setError("snapshots:agg1", storageErr)

	seed := order{ID: "1", Status: "Pending"}
	events := []internalEvent{
		{
			ID: "e1", Version: 1, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		},
	}

	state, err := r.hydrate(context.Background(), "agg1", seed, events)
	if err == nil {
		t.Fatal("expected error from auto-snapshot failure")
	}
	// State must still be correct despite snapshot failure.
	if state.Status != "Shipped" {
		t.Errorf("state.Status = %q, want Shipped (state correct despite error)", state.Status)
	}
}

func TestReplayerHydrate_UpcasterError_PropagatesFast(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	upcastErr := errors.New("upcaster broken")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) {
			return nil, upcastErr
		},
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	events := []internalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
		{ID: "e2", Version: 2, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.hydrate(context.Background(), "agg1", order{}, events)
	if err == nil {
		t.Fatal("expected upcaster error to propagate")
	}
	if !errors.Is(err, upcastErr) {
		t.Errorf("expected wrapped upcastErr, got %v", err)
	}
}

func TestReplayerHydrate_UpcasterPanic_Propagates(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) {
			panic("upcaster panic")
		},
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	events := []internalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	defer func() {
		if rec := recover(); rec == nil {
			panic("expected upcaster panic to propagate, but it was swallowed")
		}
	}()

	r.hydrate(context.Background(), "agg1", order{}, events) //nolint:errcheck
}

// --- Replayer.Replay tests ---

func TestReplayerReplay_AllEvents(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Seed event store with two events.
	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)

	var events []asynxmd.Event[order]
	err := r.Replay(context.Background(), "agg1", 1, 0, func(e asynxmd.Event[order]) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Event 1: Pending
	if events[0].Aggregate.Status != "Pending" {
		t.Errorf("events[0] Status = %q, want Pending", events[0].Aggregate.Status)
	}
	if events[0].PreviousAggregate.Status != "" {
		t.Errorf("events[0] PreviousAggregate.Status = %q, want empty (zero value)", events[0].PreviousAggregate.Status)
	}

	// Event 2: Shipped
	if events[1].Aggregate.Status != "Shipped" {
		t.Errorf("events[1] Status = %q, want Shipped", events[1].Aggregate.Status)
	}
	if events[1].PreviousAggregate.Status != "Pending" {
		t.Errorf("events[1] PreviousAggregate.Status = %q, want Pending", events[1].PreviousAggregate.Status)
	}
}

func TestReplayerReplay_WithVersionRange(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	seedOrderEvents(t, es, "agg1")

	r := newTestReplayer(es, ss, nil, 1)

	var events []asynxmd.Event[order]
	err := r.Replay(context.Background(), "agg1", 2, 2, func(e asynxmd.Event[order]) {
		events = append(events, e)
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// Only event 2 should be returned (but we need to hydrate from the seed state).
	// Since Replay starts from zero value at fromVersion, event 1 patches are skipped —
	// so state at event 2 is built only from event 2's patch applied to zero value.
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
}

func TestReplayerReplay_StorageError(t *testing.T) {
	es := &errStore{err: storageErr}
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	err := r.Replay(context.Background(), "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err == nil {
		t.Fatal("expected storage error from Replay")
	}
}

func TestReplayerReplay_NoAutoSnapshot(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	// Event at schema version 1 — needs upcasting.
	blob := makeInternalEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))
	es.Append(context.Background(), "events:agg1", 1, blob) //nolint:errcheck

	err := r.Replay(context.Background(), "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// Replay must NOT write auto-snapshots.
	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 0 {
		t.Error("Replay must not write auto-snapshots")
	}
}

// --- Replayer.upcastInternalEvent tests ---

func TestReplayerUpcast_ChainApplied(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	var calls []int
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) {
			calls = append(calls, 1)
			return p, nil
		},
		2: func(_ context.Context, _ string, p []byte) ([]byte, error) {
			calls = append(calls, 2)
			return p, nil
		},
	}
	r := newTestReplayer(es, ss, upcasters, 3)

	evt := internalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("upcastInternalEvent: %v", err)
	}
	if upcasted.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", upcasted.SchemaVersion)
	}
	if len(calls) != 2 || calls[0] != 1 || calls[1] != 2 {
		t.Errorf("upcaster chain calls = %v, want [1 2]", calls)
	}
}

func TestReplayerUpcast_MissingUpcasterSkipped(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	// Only upcaster for 2→3 registered; 1→2 is missing.
	upcasters := map[int]asynxmd.Upcaster{
		2: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(es, ss, upcasters, 3)

	evt := internalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("upcastInternalEvent: %v", err)
	}
	// SchemaVersion advances from 1 to 3 (skipping missing upcaster at 1).
	if upcasted.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", upcasted.SchemaVersion)
	}
}

func TestReplayerUpcast_ErrorWrapped(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()

	innerErr := errors.New("inner")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) {
			return nil, innerErr
		},
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	evt := internalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	_, err := r.upcastInternalEvent(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error from upcaster")
	}
	if !errors.Is(err, innerErr) {
		t.Errorf("error = %v, expected to wrap innerErr", err)
	}
}

// --- Replayer.applyPatches tests ---

func TestReplayerApplyPatches_Replace(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	state := order{Status: "Pending", Total: 100}
	patches := json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)

	result, err := r.applyPatches(state, patches)
	if err != nil {
		t.Fatalf("applyPatches: %v", err)
	}
	if result.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", result.Status)
	}
	if result.Total != 100 {
		t.Errorf("Total = %v, want 100 (unchanged)", result.Total)
	}
}

func TestReplayerApplyPatches_NilPatches(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	state := order{Status: "Pending"}
	result, err := r.applyPatches(state, nil)
	if err != nil {
		t.Fatalf("applyPatches(nil): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (unchanged)", result.Status)
	}
}

func TestReplayerApplyPatches_EmptyPatches(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	state := order{Status: "Pending"}
	result, err := r.applyPatches(state, json.RawMessage(`[]`))
	if err != nil {
		t.Fatalf("applyPatches([]): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (unchanged)", result.Status)
	}
}

func TestReplayerApplyPatches_InvalidJSON(t *testing.T) {
	es := newMemStore()
	ss := newMemStore()
	r := newTestReplayer(es, ss, nil, 1)

	_, err := r.applyPatches(order{}, json.RawMessage(`not-json`))
	if err == nil {
		t.Fatal("expected error on invalid patch JSON")
	}
}

// --- Helpers ---

// seedOrderEvents adds two events to es for aggregateID:
// v1: creates an order with Status=Pending
// v2: ships the order (Status=Shipped)
func seedOrderEvents(t *testing.T, es *memStore, aggregateID string) {
	t.Helper()

	ctx := context.Background()
	eventKey := "events:" + aggregateID

	patch1 := fmt.Sprintf(
		`[{"op":"replace","path":"/Status","value":"Pending"},{"op":"replace","path":"/Total","value":100},{"op":"replace","path":"/ID","value":"%s"}]`,
		aggregateID,
	)
	es.Append(ctx, eventKey, 1, makeInternalEventBlob(t, "e1", "OrderCreated", 1, 1, json.RawMessage(patch1))) //nolint:errcheck

	patch2 := `[{"op":"replace","path":"/Status","value":"Shipped"}]`
	es.Append(ctx, eventKey, 2, makeInternalEventBlob(t, "e2", "OrderShipped", 2, 1, json.RawMessage(patch2))) //nolint:errcheck
}

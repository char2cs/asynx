package replayer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

// --- Helpers ---

func newTestReplayer(es, ss asynxmd.Store, upcasters map[int]asynxmd.Upcaster, schemaVersion int) *Replayer[order] {
	if upcasters == nil {
		upcasters = make(map[int]asynxmd.Upcaster)
	}
	return &Replayer[order]{
		EventStore:           es,
		SnapshotStore:        ss,
		Upcasters:            upcasters,
		CurrentSchemaVersion: schemaVersion,
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

// --- Hydrate ---

func TestHydrate_BasicPatchApplication(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	seed := order{ID: "1", Status: "Pending", Total: 50}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)},
	}

	result, err := r.Hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if result.Status != "Shipped" {
		t.Errorf("Status = %q, want Shipped", result.Status)
	}
	if result.Total != 50 {
		t.Errorf("Total = %v, want 50 (unchanged)", result.Total)
	}
}

func TestHydrate_MultipleEvents(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	seed := order{ID: "1", Status: "Pending", Total: 0}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Total","value":100}]`)},
		{ID: "e2", Version: 2, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)},
	}

	result, err := r.Hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if result.Status != "Shipped" || result.Total != 100 {
		t.Errorf("got Status=%q Total=%v, want Shipped/100", result.Status, result.Total)
	}
}

func TestHydrate_EmptyEvents_ReturnsSeed(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	seed := order{ID: "1", Status: "Pending"}
	result, err := r.Hydrate(context.Background(), "agg1", seed, nil)
	if err != nil {
		t.Fatalf("Hydrate(nil events): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (seed unchanged)", result.Status)
	}
}

func TestHydrate_EmptyPatches_NoOp(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	seed := order{Status: "Pending"}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	result, err := r.Hydrate(context.Background(), "agg1", seed, events)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending (no-op patch)", result.Status)
	}
}

func TestHydrate_WithUpcasting_WritesAutoSnapshot(t *testing.T) {
	ss := store.New()
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(store.New(), ss, upcasters, 2)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(context.Background(), "agg1", order{}, events)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 1 {
		t.Fatalf("expected 1 auto-snapshot, got %d", len(snapBlobs))
	}
}

func TestHydrate_NoAutoSnapshot_WhenNoUpcasting(t *testing.T) {
	ss := store.New()
	r := newTestReplayer(store.New(), ss, nil, 1)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(context.Background(), "agg1", order{}, events)
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 0 {
		t.Error("expected no auto-snapshot when no upcasting occurred")
	}
}

func TestHydrate_AutoSnapshotFail_ReturnsStateAndError(t *testing.T) {
	ss := store.New()
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(store.New(), ss, upcasters, 2)
	ss.SetError("snapshots:agg1", storageErr)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)},
	}

	state, err := r.Hydrate(context.Background(), "agg1", order{Status: "Pending"}, events)
	if err == nil {
		t.Fatal("expected error from auto-snapshot failure")
	}
	if state.Status != "Shipped" {
		t.Errorf("state.Status = %q, want Shipped (state correct despite error)", state.Status)
	}
}

func TestHydrate_AutoSnapshot_MarshalCurrentError(t *testing.T) {
	// mocks.ErrMarshal.MarshalJSON always fails. Patches=[] is a no-op so
	// applyPatches returns early, reaching the auto-snapshot marshal at
	// replayer.go:63–65.
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := &Replayer[mocks.ErrMarshal]{
		EventStore:           store.New(),
		SnapshotStore:        store.New(),
		Upcasters:            upcasters,
		CurrentSchemaVersion: 2,
	}

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(context.Background(), "agg1", mocks.ErrMarshal{}, events)
	if err == nil {
		t.Fatal("expected marshal error from auto-snapshot path")
	}
}

func TestHydrate_UpcasterError_PropagatesFast(t *testing.T) {
	upcastErr := errors.New("upcaster broken")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) { return nil, upcastErr },
	}
	r := newTestReplayer(store.New(), store.New(), upcasters, 2)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
		{ID: "e2", Version: 2, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(context.Background(), "agg1", order{}, events)
	if err == nil {
		t.Fatal("expected upcaster error to propagate")
	}
	if !errors.Is(err, upcastErr) {
		t.Errorf("expected wrapped upcastErr, got %v", err)
	}
}

func TestHydrate_UpcasterPanic_Propagates(t *testing.T) {
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) {
			panic("upcaster panic")
		},
	}
	r := newTestReplayer(store.New(), store.New(), upcasters, 2)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	defer func() {
		if rec := recover(); rec == nil {
			panic("expected upcaster panic to propagate, but it was swallowed")
		}
	}()
	r.Hydrate(context.Background(), "agg1", order{}, events) //nolint:errcheck
}

// --- Replay ---

func TestReplay_AllEvents(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`)))  //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Shipped", 2, 1, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))) //nolint:errcheck

	r := newTestReplayer(es, store.New(), nil, 1)

	var got []asynxmd.Event[order]
	err := r.Replay(ctx, "agg1", 1, 0, func(e asynxmd.Event[order]) {
		got = append(got, e)
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].Aggregate.Status != "Pending" {
		t.Errorf("event[0] Status = %q, want Pending", got[0].Aggregate.Status)
	}
	if got[1].Aggregate.Status != "Shipped" {
		t.Errorf("event[1] Status = %q, want Shipped", got[1].Aggregate.Status)
	}
	if got[1].PreviousAggregate.Status != "Pending" {
		t.Errorf("event[1] PreviousAggregate.Status = %q, want Pending", got[1].PreviousAggregate.Status)
	}
}

func TestReplay_WithVersionRange(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck
	es.Append(ctx, "events:agg1", 2, makeEventBlob(t, "e2", "Updated", 2, 1, json.RawMessage(`[]`))) //nolint:errcheck

	r := newTestReplayer(es, store.New(), nil, 1)

	var got []asynxmd.Event[order]
	err := r.Replay(ctx, "agg1", 2, 2, func(e asynxmd.Event[order]) { got = append(got, e) })
	if err != nil {
		t.Fatalf("Replay with range: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
}

func TestReplay_StorageError(t *testing.T) {
	r := newTestReplayer(&mocks.ErrStore{Err: storageErr}, store.New(), nil, 1)

	err := r.Replay(context.Background(), "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err == nil {
		t.Fatal("expected storage error")
	}
}

func TestReplay_ReadRangeError(t *testing.T) {
	r := newTestReplayer(&mocks.ErrStore{Err: storageErr}, store.New(), nil, 1)

	err := r.Replay(context.Background(), "agg1", 1, 2, func(asynxmd.Event[order]) {})
	if !errors.Is(err, storageErr) {
		t.Errorf("err = %v, want storageErr", err)
	}
}

func TestReplay_CorruptEventBlob(t *testing.T) {
	r := newTestReplayer(&mocks.CorruptBlobStore{}, store.New(), nil, 1)

	err := r.Replay(context.Background(), "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err == nil {
		t.Fatal("expected error on corrupt event blob")
	}
}

func TestReplay_NoAutoSnapshot(t *testing.T) {
	es := store.New()
	ss := store.New()
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(es, ss, upcasters, 2)

	es.Append(context.Background(), "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	err := r.Replay(context.Background(), "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	snapBlobs, _ := ss.ReadFrom(context.Background(), "snapshots:agg1", 0)
	if len(snapBlobs) != 0 {
		t.Error("Replay must not write auto-snapshots")
	}
}

// --- upcastInternalEvent ---

func TestUpcast_ChainApplied(t *testing.T) {
	var calls []int
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, p []byte) ([]byte, error) { calls = append(calls, 1); return p, nil },
		2: func(_ context.Context, _ string, p []byte) ([]byte, error) { calls = append(calls, 2); return p, nil },
	}
	r := newTestReplayer(store.New(), store.New(), upcasters, 3)

	evt := esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("upcastInternalEvent: %v", err)
	}
	if upcasted.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", upcasted.SchemaVersion)
	}
	if len(calls) != 2 || calls[0] != 1 || calls[1] != 2 {
		t.Errorf("chain calls = %v, want [1 2]", calls)
	}
}

func TestUpcast_MissingUpcasterSkipped(t *testing.T) {
	upcasters := map[int]asynxmd.Upcaster{
		2: func(_ context.Context, _ string, p []byte) ([]byte, error) { return p, nil },
	}
	r := newTestReplayer(store.New(), store.New(), upcasters, 3)

	evt := esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(context.Background(), evt)
	if err != nil {
		t.Fatalf("upcastInternalEvent: %v", err)
	}
	if upcasted.SchemaVersion != 3 {
		t.Errorf("SchemaVersion = %d, want 3", upcasted.SchemaVersion)
	}
}

func TestUpcast_ErrorWrapped(t *testing.T) {
	inner := errors.New("inner")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) { return nil, inner },
	}
	r := newTestReplayer(store.New(), store.New(), upcasters, 2)

	_, err := r.upcastInternalEvent(context.Background(), esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, inner) {
		t.Errorf("error = %v, expected to wrap inner", err)
	}
}

// --- applyPatches ---

func TestApplyPatches_Replace(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	result, err := r.applyPatches(order{Status: "Pending", Total: 100}, json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`))
	if err != nil {
		t.Fatalf("applyPatches: %v", err)
	}
	if result.Status != "Shipped" || result.Total != 100 {
		t.Errorf("got Status=%q Total=%v, want Shipped/100", result.Status, result.Total)
	}
}

func TestApplyPatches_Nil_NoOp(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	result, err := r.applyPatches(order{Status: "Pending"}, nil)
	if err != nil {
		t.Fatalf("applyPatches(nil): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", result.Status)
	}
}

func TestApplyPatches_Null_NoOp(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	result, err := r.applyPatches(order{Status: "Pending"}, json.RawMessage("null"))
	if err != nil {
		t.Fatalf("applyPatches(null): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", result.Status)
	}
}

func TestApplyPatches_Empty_NoOp(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	result, err := r.applyPatches(order{Status: "Pending"}, json.RawMessage(`[]`))
	if err != nil {
		t.Fatalf("applyPatches([]): %v", err)
	}
	if result.Status != "Pending" {
		t.Errorf("Status = %q, want Pending", result.Status)
	}
}

func TestApplyPatches_InvalidJSON(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	_, err := r.applyPatches(order{}, json.RawMessage(`not-json`))
	if err == nil {
		t.Fatal("expected error on invalid patch JSON")
	}
}

func TestApplyPatches_MarshalStateError(t *testing.T) {
	// mocks.ErrMarshal.MarshalJSON always fails; non-empty valid patch is needed
	// to reach the json.Marshal(state) call at replayer.go:194–196.
	r := &Replayer[mocks.ErrMarshal]{
		EventStore:           store.New(),
		SnapshotStore:        store.New(),
		Upcasters:            make(map[int]asynxmd.Upcaster),
		CurrentSchemaVersion: 1,
	}

	_, err := r.applyPatches(mocks.ErrMarshal{}, json.RawMessage(`[{"op":"replace","path":"/foo","value":"bar"}]`))
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestApplyPatches_UnmarshalResultError(t *testing.T) {
	// mocks.BadUnmarshal.MarshalJSON succeeds ({}) so the patch can be applied,
	// but UnmarshalJSON always fails triggering replayer.go:204–206.
	r := &Replayer[mocks.BadUnmarshal]{
		EventStore:           store.New(),
		SnapshotStore:        store.New(),
		Upcasters:            make(map[int]asynxmd.Upcaster),
		CurrentSchemaVersion: 1,
	}

	_, err := r.applyPatches(mocks.BadUnmarshal{}, json.RawMessage(`[{"op":"add","path":"/x","value":1}]`))
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestApplyPatches_PatchApplyError(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	_, err := r.applyPatches(order{}, json.RawMessage(`[{"op":"replace","path":"/NonExistent","value":"x"}]`))
	if err == nil {
		t.Fatal("expected patch.Apply error on non-existent path")
	}
}

func TestHydrate_ApplyPatchesError(t *testing.T) {
	r := newTestReplayer(store.New(), store.New(), nil, 1)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/NonExistent","value":"x"}]`)},
	}

	_, err := r.Hydrate(context.Background(), "agg1", order{}, events)
	if err == nil {
		t.Fatal("expected error from failed patch application in Hydrate")
	}
}

func TestReplay_UpcasterError(t *testing.T) {
	upcastErr := errors.New("upcaster broken")
	upcasters := map[int]asynxmd.Upcaster{
		1: func(_ context.Context, _ string, _ []byte) ([]byte, error) { return nil, upcastErr },
	}
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1, json.RawMessage(`[]`))) //nolint:errcheck

	r := newTestReplayer(es, store.New(), upcasters, 2)

	err := r.Replay(ctx, "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if !errors.Is(err, upcastErr) {
		t.Errorf("err = %v, want upcastErr", err)
	}
}

func TestReplay_ApplyPatchesError(t *testing.T) {
	es := store.New()
	ctx := context.Background()

	es.Append(ctx, "events:agg1", 1, makeEventBlob(t, "e1", "Created", 1, 1,
		json.RawMessage(`[{"op":"replace","path":"/NonExistent","value":"x"}]`))) //nolint:errcheck

	r := newTestReplayer(es, store.New(), nil, 1)

	err := r.Replay(ctx, "agg1", 1, 0, func(asynxmd.Event[order]) {})
	if err == nil {
		t.Fatal("expected error from failed patch application in Replay")
	}
}

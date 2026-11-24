package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/eventstore/internal/replayer"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"github.com/wI2L/jsondiff"
)

type order = mocks.Order

var storageErr = errors.New("storage failure")

func newTestReader(
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
) *Reader[order] {
	rep := replayer.New[order](
		es,
		make(map[int]asynxmd.Upcaster),
		1,
		order{},
	)
	return New[order](
		es,
		ss,
		rep,
		1,
		order{},
		nil,
	)
}

func makeEventBlob(
	t *testing.T,
	id,
	eventName string,
	version int64,
	schemaVersion int,
	patches json.RawMessage,
) []byte {
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
		t.Fatalf(
			"makeEventBlob: %v",
			err,
		)
	}

	return b
}

func makeSnapshotBlob(
	t *testing.T,
	version int64,
	schemaVersion int,
	state order,
) []byte {
	t.Helper()
	snap := esmodels.SnapshotBlob[order]{
		Version:       version,
		SchemaVersion: schemaVersion,
		State:         state,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf(
			"makeSnapshotBlob: %v",
			err,
		)
	}

	return b
}

func TestGet_ColdPath_ReturnsHydratedState(
	t *testing.T,
) {
	es := store.New()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`),
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		2,
		makeEventBlob(
			t,
			"e2",
			"Updated",
			2,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	r := newTestReader(
		es,
		store.NewSnapshots(),
	)

	got, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Shipped" {
		t.Errorf(
			"Status = %q, want Shipped",
			got.Status,
		)
	}
}

func TestGet_ColdPath_ErrNotFound_WhenNoEvents(
	t *testing.T,
) {
	r := newTestReader(
		store.New(),
		store.NewSnapshots(),
	)

	_, err := r.Get(
		context.Background(),
		"missing",
	)
	if !errors.Is(
		err,
		asynxmd.ErrNotFound,
	) {
		t.Errorf(
			"err = %v, want ErrNotFound",
			err,
		)
	}
}

func TestGet_ColdPath_StorageError(
	t *testing.T,
) {
	r := newTestReader(
		&mockstores.ErrStore{Err: storageErr},
		store.NewSnapshots(),
	)

	_, err := r.Get(
		context.Background(),
		"agg1",
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

func TestGet_ColdPath_DeserializeError(
	t *testing.T,
) {
	r := newTestReader(
		&mockstores.CorruptBlobStore{},
		store.NewSnapshots(),
	)

	_, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err == nil {
		t.Fatal("expected deserialize error on corrupt event blobs")
	}
}

func TestGet_WarmPath_SnapshotPlusDelta(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		makeSnapshotBlob(
			t,
			1,
			1,
			order{ID: "1", Status: "Pending", Total: 50},
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		2,
		makeEventBlob(
			t,
			"e2",
			"Shipped",
			2,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	r := newTestReader(
		es,
		ss,
	)

	got, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get warm path: %v",
			err,
		)
	}

	if got.Status != "Shipped" {
		t.Errorf(
			"Status = %q, want Shipped",
			got.Status,
		)
	}

	if got.Total != 50 {
		t.Errorf(
			"Total = %v, want 50 (from snapshot)",
			got.Total,
		)
	}
}

func TestGet_WarmPath_NoDelta_ReturnsSnapshot(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		makeSnapshotBlob(
			t,
			1,
			1,
			order{Status: "Pending"},
		),
	)

	r := newTestReader(
		store.New(),
		ss,
	)

	got, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Pending" {
		t.Errorf(
			"Status = %q, want Pending",
			got.Status,
		)
	}
}

func TestGet_WarmPath_CorruptSnapshot_FallsBackToCold(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		[]byte(`not-valid-json`),
	)
	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`),
		),
	)

	r := newTestReader(
		es,
		ss,
	)

	got, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get with corrupt snapshot: %v",
			err,
		)
	}

	if got.Status != "Pending" {
		t.Errorf(
			"Status = %q, want Pending (cold path fallback)",
			got.Status,
		)
	}
}

func TestGet_WarmPath_CorruptSnapshot_CallsOnCorruption(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		[]byte(`{"version":1,"schema_version":1,"state":"not an order object"}`),
	)
	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	var hookErr error
	rep := replayer.New[order](
		es,
		make(map[int]asynxmd.Upcaster),
		1,
		order{},
	)
	newFn := func(
		err error,
	) {
		hookErr = err
	}
	r := New[order](
		es,
		ss,
		rep,
		1,
		order{},
		newFn,
	)

	_, _ = r.Get(
		ctx,
		"agg1",
	)
	if hookErr == nil {
		t.Error("expected onCorruption to be called with an error")
	}
}

func TestGet_WarmPath_CorruptStateInSnapshot_FallsBackToCold(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		[]byte(`{"version":1,"schema_version":1,"state":"not an order object"}`),
	)
	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	r := newTestReader(
		es,
		ss,
	)

	got, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Shipped" {
		t.Errorf(
			"Status = %q, want Shipped (cold path fallback)",
			got.Status,
		)
	}
}

func TestGet_WarmPath_DeltaDeserializeError(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		makeSnapshotBlob(
			t,
			1,
			1,
			order{Status: "Pending"},
		),
	)

	r := newTestReader(
		&mockstores.CorruptBlobStore{},
		ss,
	)

	_, err := r.Get(
		ctx,
		"agg1",
	)
	if err == nil {
		t.Fatal("expected deserialize error on corrupt delta blobs")
	}
}

func TestGet_WarmPath_DeltaStorageError(
	t *testing.T,
) {
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		makeSnapshotBlob(
			t,
			1,
			1,
			order{Status: "Pending"},
		),
	)

	r := newTestReader(
		&mockstores.ErrStore{Err: storageErr},
		ss,
	)

	_, err := r.Get(
		ctx,
		"agg1",
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

func TestGet_SnapshotStorageError(
	t *testing.T,
) {
	r := newTestReader(
		store.New(),
		&failingSnapshots{err: storageErr},
	)

	_, err := r.Get(
		context.Background(),
		"agg1",
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

func TestGet_AutoSnapshotFail_ReturnsState(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	rep := replayer.New[order](
		es,
		upcasters,
		2,
		order{},
	)
	r := New[order](
		es,
		ss,
		rep,
		2,
		order{},
		nil,
	)

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	ss.SetError(
		"agg1",
		storageErr,
	)

	state, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: expected no error, got %v",
			err,
		)
	}

	if state.Status != "Shipped" {
		t.Errorf(
			"state.Status = %q, want Shipped (state from events)",
			state.Status,
		)
	}
}

func TestGet_AutoSnapshot_WrittenAfterUpcasting(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	rep := replayer.New[order](
		es,
		upcasters,
		2,
		order{},
	)
	r := New[order](
		es,
		ss,
		rep,
		2,
		order{},
		nil,
	)

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	_, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	_, found, _ := ss.Get(
		ctx,
		"agg1",
	)
	if !found {
		t.Fatal("expected 1 auto-snapshot written, got none")
	}
}

func TestExists_ReturnsTrueWhenEventExists(
	t *testing.T,
) {
	es := store.New()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := newTestReader(
		es,
		store.NewSnapshots(),
	)

	ok, err := r.Exists(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Exists: %v",
			err,
		)
	}

	if !ok {
		t.Error("Exists = false, want true")
	}
}

func TestExists_ReturnsFalseWhenNoEvents(
	t *testing.T,
) {
	r := newTestReader(
		store.New(),
		store.NewSnapshots(),
	)

	ok, err := r.Exists(
		context.Background(),
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Exists: %v",
			err,
		)
	}

	if ok {
		t.Error("Exists = true, want false")
	}
}

func TestExists_StorageError(
	t *testing.T,
) {
	r := newTestReader(
		&mockstores.ErrStore{Err: storageErr},
		store.NewSnapshots(),
	)

	_, err := r.Exists(
		context.Background(),
		"agg1",
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

func TestPreload_NoError_WhenAggregateExists(
	t *testing.T,
) {
	es := store.New()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := newTestReader(
		es,
		store.NewSnapshots(),
	)

	if err := r.Preload(
		ctx,
		"agg1",
	); err != nil {
		t.Errorf(
			"Preload: %v",
			err,
		)
	}
}

func TestPreload_NoError_WhenAggregateNotFound(
	t *testing.T,
) {
	r := newTestReader(
		store.New(),
		store.NewSnapshots(),
	)

	if err := r.Preload(
		context.Background(),
		"missing",
	); err != nil {
		t.Errorf(
			"Preload on missing aggregate: %v (want nil)",
			err,
		)
	}
}

func TestPreload_PropagatesStorageError(
	t *testing.T,
) {
	r := newTestReader(
		&mockstores.ErrStore{Err: storageErr},
		store.NewSnapshots(),
	)

	err := r.Preload(
		context.Background(),
		"agg1",
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

func TestGet_WarmPath_AutoSnapshotError_ReturnsStateNotError(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	ss.Put(
		ctx,
		"agg1",
		1,
		makeSnapshotBlob(
			t,
			1,
			1,
			order{Status: "Old"},
		),
	)

	es.Append(
		ctx,
		"events:agg1",
		2,
		makeEventBlob(
			t,
			"e1",
			"Updated",
			2,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"New"}]`),
		),
	)
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	rep := replayer.New[order](
		es,
		upcasters,
		2,
		order{},
	)
	r := New[order](
		es,
		ss,
		rep,
		2,
		order{},
		nil,
	)

	ss.SetError(
		"agg1",
		storageErr,
	)

	state, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: expected no error, got %v",
			err,
		)
	}

	if state.Status != "New" {
		t.Errorf(
			"Status = %q, want New (correct state from events)",
			state.Status,
		)
	}
}

func TestGet_ColdPath_AutoSnapshotError_ReturnsStateNotError(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`),
		),
	)
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	rep := replayer.New[order](
		es,
		upcasters,
		2,
		order{},
	)
	r := New[order](
		es,
		ss,
		rep,
		2,
		order{},
		nil,
	)

	ss.SetError(
		"agg1",
		storageErr,
	)

	state, err := r.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: expected no error, got %v",
			err,
		)
	}

	if state.Status != "Pending" {
		t.Errorf(
			"Status = %q, want Pending (correct state from events)",
			state.Status,
		)
	}
}

func TestGet_ColdPath_HydrateError_ReturnsError(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()

	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Pending"}]`),
		),
	)

	sentinel := errors.New("upcaster failed")
	fn := func(
		_ context.Context,
		_ string,
		_ []byte,
	) ([]byte, error) {
		return nil,
			sentinel
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	rep := replayer.New[order](
		es,
		upcasters,
		2,
		order{},
	)
	r := New[order](
		es,
		ss,
		rep,
		2,
		order{},
		nil,
	)

	_, err := r.Get(
		ctx,
		"agg1",
	)
	if !errors.Is(
		err,
		sentinel,
	) {
		t.Fatalf(
			"expected sentinel upcaster error, got %v",
			err,
		)
	}
}

func writeEvent(
	t *testing.T,
	s store.Memory,
	id string,
	version int64,
	state order,
) {
	t.Helper()
	patch, err := jsondiff.Compare(
		order{},
		state,
	)
	if err != nil {
		t.Fatalf(
			"diff: %v",
			err,
		)
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		t.Fatalf(
			"marshal patch: %v",
			err,
		)
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
		t.Fatalf(
			"marshal event: %v",
			err,
		)
	}

	if err := s.Append(
		context.Background(),
		"events:"+id,
		version,
		blob,
	); err != nil {
		t.Fatalf(
			"append: %v",
			err,
		)
	}
}

func TestReader_ReadsSnapshotExactlyOnce(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := &mockstores.CountingSnapshotStore{Inner: store.NewSnapshots()}
	rep := replayer.New[order](
		events,
		nil,
		1,
		order{},
	)
	r := New[order](
		events,
		snaps,
		rep,
		1,
		order{},
		nil,
	)

	writeEvent(
		t,
		events,
		"agg-1",
		1,
		order{ID: "agg-1", Total: 1, Status: "Pending"},
	)
	blob := makeSnapshotBlob(
		t,
		1,
		1,
		order{ID: "agg-1", Total: 1, Status: "Pending"},
	)
	if err := snaps.Put(
		ctx,
		"agg-1",
		1,
		blob,
	); err != nil {
		t.Fatalf(
			"put: %v",
			err,
		)
	}

	snaps.Reset()
	if _, err := r.Get(
		ctx,
		"agg-1",
	); err != nil {
		t.Fatalf(
			"get: %v",
			err,
		)
	}

	if snaps.GetCalls != 1 {
		t.Errorf(
			"snapshot Get calls = %d, want exactly 1",
			snaps.GetCalls,
		)
	}

	if snaps.GetBytes != len(blob) {
		t.Errorf(
			"bytes read = %d, want %d (exactly one blob)",
			snaps.GetBytes,
			len(blob),
		)
	}
}

func TestReader_MissingSnapshotTakesColdPath(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](
		events,
		nil,
		1,
		order{},
	)
	r := New[order](
		events,
		snaps,
		rep,
		1,
		order{},
		nil,
	)

	writeEvent(
		t,
		events,
		"agg-1",
		1,
		order{ID: "agg-1", Total: 42, Status: "Pending"},
	)

	got, err := r.Get(
		ctx,
		"agg-1",
	)
	if err != nil {
		t.Fatalf(
			"get: %v",
			err,
		)
	}

	if got.Total != 42 {
		t.Errorf(
			"Total = %v, want 42",
			got.Total,
		)
	}
}

func TestReader_CorruptSnapshotFallsBackToColdPath(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](
		events,
		nil,
		1,
		order{},
	)

	var corruptionErr error
	newFn := func(
		err error,
	) {
		corruptionErr = err
	}
	r := New[order](
		events,
		snaps,
		rep,
		1,
		order{},
		newFn,
	)

	writeEvent(
		t,
		events,
		"agg-1",
		1,
		order{ID: "agg-1", Total: 42, Status: "Pending"},
	)
	if err := snaps.Put(
		ctx,
		"agg-1",
		1,
		[]byte("{not json"),
	); err != nil {
		t.Fatalf(
			"put: %v",
			err,
		)
	}

	got, err := r.Get(
		ctx,
		"agg-1",
	)
	if err != nil {
		t.Fatalf(
			"get: %v",
			err,
		)
	}

	if got.Total != 42 {
		t.Errorf(
			"Total = %v, want 42 — corrupt snapshot must cold-replay",
			got.Total,
		)
	}

	if corruptionErr == nil {
		t.Error("onCorruption was not called")
	}
}

func TestReader_SnapshotStoreErrorPropagates(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := &failingSnapshots{err: storageErr}
	rep := replayer.New[order](
		events,
		nil,
		1,
		order{},
	)
	r := New[order](
		events,
		snaps,
		rep,
		1,
		order{},
		nil,
	)

	if _, err := r.Get(
		ctx,
		"agg-1",
	); err == nil {
		t.Fatal("err = nil, want storage error")
	}
}

func TestReader_StaleSnapshotStillHydratesCorrectState(
	t *testing.T,
) {
	ctx := context.Background()
	events := store.New()
	snaps := store.NewSnapshots()
	rep := replayer.New[order](
		events,
		nil,
		1,
		order{},
	)
	r := New[order](
		events,
		snaps,
		rep,
		1,
		order{},
		nil,
	)

	writeEvent(
		t,
		events,
		"agg-1",
		1,
		order{ID: "agg-1", Total: 1, Status: "Pending"},
	)
	writeEvent(
		t,
		events,
		"agg-1",
		2,
		order{ID: "agg-1", Total: 2, Status: "Pending"},
	)
	writeEvent(
		t,
		events,
		"agg-1",
		3,
		order{ID: "agg-1", Total: 3, Status: "Pending"},
	)
	writeEvent(
		t,
		events,
		"agg-1",
		4,
		order{ID: "agg-1", Total: 4, Status: "Pending"},
	)
	writeEvent(
		t,
		events,
		"agg-1",
		5,
		order{ID: "agg-1", Total: 5, Status: "Shipped"},
	)

	stale := makeSnapshotBlob(
		t,
		2,
		1,
		order{ID: "agg-1", Total: 2, Status: "Pending"},
	)
	if err := snaps.Put(
		ctx,
		"agg-1",
		2,
		stale,
	); err != nil {
		t.Fatalf(
			"put: %v",
			err,
		)
	}

	got, err := r.Get(
		ctx,
		"agg-1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	want := order{ID: "agg-1", Total: 5, Status: "Shipped"}
	if got != want {
		t.Errorf(
			"Get with stale snapshot = %+v, want %+v (current state, not stale v2 snapshot)",
			got,
			want,
		)
	}
}

type failingSnapshots struct{ err error }

// Put returns the configured error.
func (f *failingSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return f.err
}

// Get returns the configured error.
func (f *failingSnapshots) Get(
	context.Context,
	string,
) ([]byte, bool, error) {
	return nil,
		false,
		f.err
}

// Delete returns the configured error.
func (f *failingSnapshots) Delete(
	context.Context,
	string,
) error {
	return f.err
}

type afterReadStore struct {
	asynxmd.Store
	hook func()
	once bool
}

// ReadFrom reads from the wrapped store, then runs the hook once.
func (s *afterReadStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	blobs, err := s.Store.ReadFrom(
		ctx,
		streamID,
		fromVersion,
	)
	if !s.once {
		s.once = true
		s.hook()
	}

	return blobs,
		err
}

func upcastingReader(
	es asynxmd.Store,
	ss asynxmd.SnapshotStore,
) *Reader[order] {
	fn := func(
		_ context.Context,
		_ string,
		p []byte,
	) ([]byte, error) {
		return p,
			nil
	}
	upcasters := map[int]asynxmd.Upcaster{
		1: fn,
	}
	return New[order](
		es,
		ss,
		replayer.New[order](
			es,
			upcasters,
			2,
			order{},
		),
		2,
		order{},
		nil,
	)
}

func TestGet_AutoSnapshot_NotWrittenOverAStreamErasedMeanwhile(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)
	hook2 := func() {
		mem.Delete(
			ctx,
			"events:agg1",
		)
	}
	es := &afterReadStore{Store: mem, hook: hook2}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot of an erased aggregate was written")
	}
}

func TestGet_AutoSnapshot_NotWrittenOverAStreamRecreatedMeanwhile(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	recreate := func() {
		mem.Delete(
			ctx,
			"events:agg1",
		)
		mem.Append(
			ctx,
			"events:agg1",
			1,
			makeEventBlob(
				t,
				"e9",
				"Created",
				1,
				2,
				json.RawMessage(`[]`),
			),
		)
	}
	es := &afterReadStore{Store: mem, hook: recreate}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot of the old aggregate was written over the re-created one")
	}
}

func TestGet_AutoSnapshot_NotWrittenWhenTheStreamCannotBeReChecked(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	es := &readRangeFailsStore{Store: mem}
	if _, err := upcastingReader(
		es,
		ss,
	).Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot was written without confirming the stream")
	}

	mem.Append(
		ctx,
		"events:agg2",
		1,
		[]byte("not json"),
	)
	r := upcastingReader(
		mem,
		ss,
	)
	if r.streamHolds(
		ctx,
		"agg2",
		1,
		"e1",
	) {
		t.Fatal("an unreadable event was taken for the expected one")
	}
}

type readRangeFailsStore struct{ asynxmd.Store }

// ReadRange always fails with the shared storage error.
func (readRangeFailsStore) ReadRange(
	context.Context,
	string,
	int64,
	int64,
) ([][]byte, error) {
	return nil,
		storageErr
}

func assertNoSnapshot(
	t *testing.T,
	ss asynxmd.SnapshotStore,
) {
	t.Helper()

	_, found, _ := ss.Get(
		context.Background(),
		"agg1",
	)
	if found {
		t.Error("the snapshot was written before the guard ran")
	}
}

func TestGet_AutoSnapshot_IsWrittenUnderTheGuard(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	var guarded []string
	setSnapshotGuardFn := func(
		id string,
		fn func(),
	) bool {
		guarded = append(
			guarded,
			id,
		)
		assertNoSnapshot(
			t,
			ss,
		)
		fn()
		return true
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, err := r.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if len(guarded) != 1 ||
		guarded[0] != "agg1" {
		t.Fatalf(
			"guard calls = %v, want [agg1]",
			guarded,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("no snapshot written under the guard")
	}
}

func TestGet_AutoSnapshot_SkippedWhenTheGuardRefuses(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	setSnapshotGuardFn := func(
		string,
		func(),
	) bool {
		return false
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, err := r.Get(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); found {
		t.Fatal("a snapshot was written although the guard refused")
	}
}

func TestLoad_AutoSnapshot_DoesNotUseTheGuard(
	t *testing.T,
) {
	mem := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	mem.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			json.RawMessage(`[]`),
		),
	)

	r := upcastingReader(
		mem,
		ss,
	)
	setSnapshotGuardFn := func(
		string,
		func(),
	) bool {
		t.Error("Load asked for the guard")
		return false
	}
	r.SetSnapshotGuard(setSnapshotGuardFn)

	if _, _, err := r.Load(
		ctx,
		"agg1",
	); err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if _, found, _ := ss.Get(
		ctx,
		"agg1",
	); !found {
		t.Fatal("Load did not write the auto-snapshot")
	}
}

func TestLoad_NewAggregate_ReturnsNotFoundAndZero(
	t *testing.T,
) {
	r := newTestReader(
		store.New(),
		store.NewSnapshots(),
	)

	_, version, err := r.Load(
		context.Background(),
		"ghost",
	)
	if err != asynxmd.ErrNotFound {
		t.Fatalf(
			"err = %v, want ErrNotFound",
			err,
		)
	}

	if version != 0 {
		t.Errorf(
			"version = %d, want 0",
			version,
		)
	}
}

func TestLoad_ColdPath_ReturnsLastEventVersion(
	t *testing.T,
) {
	es := store.New()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			patch,
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		2,
		makeEventBlob(
			t,
			"e2",
			"Updated",
			2,
			1,
			patch,
		),
	)

	r := newTestReader(
		es,
		store.NewSnapshots(),
	)

	_, version, err := r.Load(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if version != 2 {
		t.Errorf(
			"version = %d, want 2 (last event)",
			version,
		)
	}
}

func TestLoad_WarmPath_ContinuesFromSnapshot(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	ss.Put(
		ctx,
		"agg1",
		5,
		makeSnapshotBlob(
			t,
			5,
			1,
			order{},
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		6,
		makeEventBlob(
			t,
			"e6",
			"U",
			6,
			1,
			patch,
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		7,
		makeEventBlob(
			t,
			"e7",
			"U",
			7,
			1,
			patch,
		),
	)

	r := newTestReader(
		es,
		ss,
	)

	_, version, err := r.Load(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if version != 7 {
		t.Errorf(
			"version = %d, want 7 (last delta after snapshot)",
			version,
		)
	}
}

func TestLoad_WarmPath_SnapshotWithNoDeltas_ReturnsSnapshotVersion(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	ss.Put(
		ctx,
		"agg1",
		5,
		makeSnapshotBlob(
			t,
			5,
			1,
			order{Status: "Snapped"},
		),
	)

	r := newTestReader(
		es,
		ss,
	)

	_, version, err := r.Load(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if version != 5 {
		t.Errorf(
			"version = %d, want 5 (snapshot version)",
			version,
		)
	}
}

func TestLoad_CorruptSnapshot_ReturnsColdPathVersion(
	t *testing.T,
) {
	es := store.New()
	ss := store.NewSnapshots()
	ctx := context.Background()
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	es.Append(
		ctx,
		"events:agg1",
		1,
		makeEventBlob(
			t,
			"e1",
			"Created",
			1,
			1,
			patch,
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		2,
		makeEventBlob(
			t,
			"e2",
			"Updated",
			2,
			1,
			patch,
		),
	)
	ss.Put(
		ctx,
		"agg1",
		1,
		[]byte("{not json"),
	)

	r := newTestReader(
		es,
		ss,
	)

	_, version, err := r.Load(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Load: %v",
			err,
		)
	}

	if version != 2 {
		t.Errorf(
			"version = %d, want 2 — a corrupt snapshot must cold-replay to the last event version",
			version,
		)
	}
}

func benchEventBlob(
	version int64,
	patches json.RawMessage,
) []byte {
	evt := esmodels.InternalEvent{
		ID:            "bench",
		EventName:     "Updated",
		Version:       version,
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC(),
		Patches:       patches,
	}
	b, err := json.Marshal(evt)
	if err != nil {
		panic(err)
	}

	return b
}

func benchSnapshotBlob(
	version int64,
	state order,
) []byte {
	snap := esmodels.SnapshotBlob[order]{
		Version:       version,
		SchemaVersion: 1,
		State:         state,
	}
	b, err := json.Marshal(snap)
	if err != nil {
		panic(err)
	}

	return b
}

func populateEvents(
	b *testing.B,
	es store.Memory,
	startVersion int,
	n int,
) {
	b.Helper()

	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	for i := 0; i < n; i++ {
		appendBenchEvent(
			b,
			es,
			int64(startVersion+i),
			patch,
		)
	}
}

func appendBenchEvent(
	b *testing.B,
	es store.Memory,
	version int64,
	patch json.RawMessage,
) {
	b.Helper()

	err := es.Append(
		context.Background(),
		"events:agg1",
		version,
		benchEventBlob(
			version,
			patch,
		),
	)
	if err != nil {
		b.Fatal(err)
	}
}

func mustGet(
	b *testing.B,
	r *Reader[order],
) {
	b.Helper()

	_, err := r.Get(
		context.Background(),
		"agg1",
	)
	if err != nil {
		b.Fatal(err)
	}
}

func benchGetLoop(
	b *testing.B,
	r *Reader[order],
) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustGet(
			b,
			r,
		)
	}
}

func benchCounts() []int {
	return []int{1, 10, 1_000_000}
}

func BenchmarkGet_WarmPath(
	b *testing.B,
) {
	for _, deltas := range benchCounts() {
		b.Run(
			fmt.Sprintf(
				"deltas=%d",
				deltas,
			),
			warmPathBench(deltas),
		)
	}
}

func seedBenchSnapshot(
	b *testing.B,
	ss asynxmd.SnapshotStore,
) {
	b.Helper()

	err := ss.Put(
		context.Background(),
		"agg1",
		1,
		benchSnapshotBlob(
			1,
			order{ID: "1", Status: "Pending", Total: 50},
		),
	)
	if err != nil {
		b.Fatal(err)
	}
}

func warmPathBench(
	deltas int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		es := store.New()
		ss := store.NewSnapshots()
		seedBenchSnapshot(
			b,
			ss,
		)
		populateEvents(
			b,
			es,
			2,
			deltas,
		)
		benchGetLoop(
			b,
			newTestReader(
				es,
				ss,
			),
		)
	}
}

func BenchmarkGet_ColdPath(
	b *testing.B,
) {
	for _, events := range benchCounts() {
		b.Run(
			fmt.Sprintf(
				"events=%d",
				events,
			),
			coldPathBench(events),
		)
	}
}

func coldPathBench(
	events int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		es := store.New()
		populateEvents(
			b,
			es,
			1,
			events,
		)
		benchGetLoop(
			b,
			newTestReader(
				es,
				store.NewSnapshots(),
			),
		)
	}
}

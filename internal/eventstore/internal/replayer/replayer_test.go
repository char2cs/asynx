package replayer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
	"github.com/char2cs/asynx/store"
)

type order = mocks.Order

type eventCollector struct {
	events []event.Event[order]
}

func (c *eventCollector) collect(
	_ context.Context,
	e event.Event[order],
) {
	c.events = append(
		c.events,
		e,
	)
}

func keepPayloadUpcaster(
	_ context.Context,
	_ string,
	payload []byte,
) ([]byte, error) {
	return payload,
		nil
}

func panickingUpcaster(
	context.Context,
	string,
	[]byte,
) ([]byte, error) {
	panic("upcaster panic")
}

type failingUpcaster struct {
	err error
}

func (u failingUpcaster) upcast(
	context.Context,
	string,
	[]byte,
) ([]byte, error) {
	return nil,
		u.err
}

type upcastStepRecorder struct {
	calls *[]int
	step  int
}

func (u upcastStepRecorder) upcast(
	_ context.Context,
	_ string,
	payload []byte,
) ([]byte, error) {
	*u.calls = append(
		*u.calls,
		u.step,
	)
	return payload,
		nil
}

var storageErr = errors.New("storage failure")

func newTestReplayer(
	es storage.Store,
	upcasters map[int]event.Upcaster,
	schemaVersion int,
) *Replayer[order] {
	return New[order](
		es,
		upcasters,
		schemaVersion,
		order{},
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

func TestHydrate_BasicPatchApplication(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	seed := order{ID: "1", Status: "Pending", Total: 50}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)},
	}

	result, err := r.Hydrate(
		context.Background(),
		seed,
		events,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate: %v",
			err,
		)
	}

	if result.State.Status != "Shipped" {
		t.Errorf(
			"Status = %q, want Shipped",
			result.State.Status,
		)
	}

	if result.State.Total != 50 {
		t.Errorf(
			"Total = %v, want 50 (unchanged)",
			result.State.Total,
		)
	}
}

func TestHydrate_MultipleEvents(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	seed := order{ID: "1", Status: "Pending", Total: 0}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Total","value":100}]`)},
		{ID: "e2", Version: 2, SchemaVersion: 1, Patches: json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`)},
	}

	result, err := r.Hydrate(
		context.Background(),
		seed,
		events,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate: %v",
			err,
		)
	}

	if result.State.Status != "Shipped" ||
		result.State.Total != 100 {
		t.Errorf(
			"got Status=%q Total=%v, want Shipped/100",
			result.State.Status,
			result.State.Total,
		)
	}
}

func TestHydrate_EmptyEvents_ReturnsSeed(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	seed := order{ID: "1", Status: "Pending"}
	result, err := r.Hydrate(
		context.Background(),
		seed,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate(nil events): %v",
			err,
		)
	}

	if result.State.Status != "Pending" {
		t.Errorf(
			"Status = %q, want Pending (seed unchanged)",
			result.State.Status,
		)
	}
}

func TestHydrate_EmptyPatches_NoOp(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	seed := order{Status: "Pending"}
	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	result, err := r.Hydrate(
		context.Background(),
		seed,
		events,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate: %v",
			err,
		)
	}

	if result.State.Status != "Pending" {
		t.Errorf(
			"Status = %q, want Pending (no-op patch)",
			result.State.Status,
		)
	}
}

func TestHydrate_WithUpcasting_WritesAutoSnapshot(
	t *testing.T,
) {
	upcasters := map[int]event.Upcaster{
		1: keepPayloadUpcaster,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		2,
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	result, err := r.Hydrate(
		context.Background(),
		order{},
		events,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate: %v",
			err,
		)
	}

	if !result.DidUpcast {
		t.Fatal("expected DidUpcast = true after upcasting")
	}

	if result.LastVersion != 1 {
		t.Errorf(
			"LastVersion = %d, want 1",
			result.LastVersion,
		)
	}
}

func TestHydrate_NoAutoSnapshot_WhenNoUpcasting(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	result, err := r.Hydrate(
		context.Background(),
		order{},
		events,
	)
	if err != nil {
		t.Fatalf(
			"Hydrate: %v",
			err,
		)
	}

	if result.DidUpcast {
		t.Error("expected DidUpcast = false when no upcasting occurred")
	}
}

func TestHydrate_MarshalSeedError(
	t *testing.T,
) {
	upcasters := map[int]event.Upcaster{
		1: keepPayloadUpcaster,
	}
	r := New[mocks.ErrMarshal](
		store.New(),
		upcasters,
		2,
		mocks.ErrMarshal{},
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(
		context.Background(),
		mocks.ErrMarshal{},
		events,
	)
	if err == nil {
		t.Fatal("expected marshal error on seed state")
	}
}

func TestHydrate_UnmarshalResultError(
	t *testing.T,
) {
	r := New[mocks.BadUnmarshal](
		store.New(),
		make(map[int]event.Upcaster),
		1,
		mocks.BadUnmarshal{},
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(
		context.Background(),
		mocks.BadUnmarshal{},
		events,
	)
	if err == nil {
		t.Fatal("expected unmarshal error after applying patches")
	}
}

func TestHydrate_UpcasterError_PropagatesFast(
	t *testing.T,
) {
	upcastErr := errors.New("upcaster broken")
	upcasters := map[int]event.Upcaster{
		1: failingUpcaster{err: upcastErr}.upcast,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		2,
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
		{ID: "e2", Version: 2, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	_, err := r.Hydrate(
		context.Background(),
		order{},
		events,
	)
	if err == nil {
		t.Fatal("expected upcaster error to propagate")
	}

	if !errors.Is(
		err,
		upcastErr,
	) {
		t.Errorf(
			"expected wrapped upcastErr, got %v",
			err,
		)
	}
}

func expectPanicPropagated() {
	rec := recover()
	if rec == nil {
		panic("expected upcaster panic to propagate, but it was swallowed")
	}
}

func TestHydrate_UpcasterPanic_Propagates(
	t *testing.T,
) {
	upcasters := map[int]event.Upcaster{
		1: panickingUpcaster,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		2,
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	}

	defer expectPanicPropagated()
	r.Hydrate(
		context.Background(),
		order{},
		events,
	)
}

func TestReplay_EmptyStore_ReturnsNil(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	var collector eventCollector
	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		0,
		collector.collect,
	)
	if err != nil {
		t.Fatalf(
			"Replay on empty store: %v",
			err,
		)
	}

	if len(collector.events) != 0 {
		t.Error("fn should not be called when store is empty")
	}
}

func TestReplay_AllEvents(
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
			"Shipped",
			2,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	r := newTestReplayer(
		es,
		nil,
		1,
	)

	var collector eventCollector
	err := r.Replay(
		ctx,
		"agg1",
		1,
		0,
		collector.collect,
	)
	if err != nil {
		t.Fatalf(
			"Replay: %v",
			err,
		)
	}

	got := collector.events
	if len(got) != 2 {
		t.Fatalf(
			"expected 2 events, got %d",
			len(got),
		)
	}

	if got[0].Aggregate.Status != "Pending" {
		t.Errorf(
			"event[0] Status = %q, want Pending",
			got[0].Aggregate.Status,
		)
	}

	if got[1].Aggregate.Status != "Shipped" {
		t.Errorf(
			"event[1] Status = %q, want Shipped",
			got[1].Aggregate.Status,
		)
	}

	if got[1].PreviousAggregate.Status != "Pending" {
		t.Errorf(
			"event[1] PreviousAggregate.Status = %q, want Pending",
			got[1].PreviousAggregate.Status,
		)
	}
}

func TestReplay_WithVersionRange(
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
			json.RawMessage(`[]`),
		),
	)

	r := newTestReplayer(
		es,
		nil,
		1,
	)

	var collector eventCollector
	err := r.Replay(
		ctx,
		"agg1",
		2,
		2,
		collector.collect,
	)
	if err != nil {
		t.Fatalf(
			"Replay with range: %v",
			err,
		)
	}

	got := collector.events
	if len(got) != 1 {
		t.Fatalf(
			"expected 1 event, got %d",
			len(got),
		)
	}
}

func TestReplay_PreviousAggregate_CorrectForMidStreamStart(
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
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Created"}]`),
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
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Updated"}]`),
		),
	)
	es.Append(
		ctx,
		"events:agg1",
		3,
		makeEventBlob(
			t,
			"e3",
			"Shipped",
			3,
			1,
			json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
		),
	)

	r := newTestReplayer(
		es,
		nil,
		1,
	)

	var collector eventCollector
	err := r.Replay(
		ctx,
		"agg1",
		3,
		3,
		collector.collect,
	)
	if err != nil {
		t.Fatalf(
			"Replay: %v",
			err,
		)
	}

	got := collector.events
	if len(got) != 1 {
		t.Fatalf(
			"expected 1 event, got %d",
			len(got),
		)
	}

	event := got[0]
	if event.Version != 3 {
		t.Errorf(
			"Version = %d, want 3",
			event.Version,
		)
	}

	if event.PreviousAggregate.Status != "Updated" {
		t.Errorf(
			"PreviousAggregate.Status = %q, want Updated (state from event 2)",
			event.PreviousAggregate.Status,
		)
	}

	if event.Aggregate.Status != "Shipped" {
		t.Errorf(
			"Aggregate.Status = %q, want Shipped (state after event 3)",
			event.Aggregate.Status,
		)
	}
}

func TestReplay_StorageError(
	t *testing.T,
) {
	r := newTestReplayer(
		&mockstores.ErrStore{Err: storageErr},
		nil,
		1,
	)

	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[order],
		) {
		},
	)
	if err == nil {
		t.Fatal("expected storage error")
	}
}

func TestReplay_ReadRangeError(
	t *testing.T,
) {
	r := newTestReplayer(
		&mockstores.ErrStore{Err: storageErr},
		nil,
		1,
	)

	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		2,
		new(eventCollector).collect,
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

func TestReplay_CorruptEventBlob(
	t *testing.T,
) {
	r := newTestReplayer(
		&mockstores.CorruptBlobStore{},
		nil,
		1,
	)

	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[order],
		) {
		},
	)
	if err == nil {
		t.Fatal("expected error on corrupt event blob")
	}
}

func TestReplay_NoAutoSnapshot(
	t *testing.T,
) {
	es := store.New()
	upcasters := map[int]event.Upcaster{
		1: keepPayloadUpcaster,
	}
	r := newTestReplayer(
		es,
		upcasters,
		2,
	)

	es.Append(
		context.Background(),
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

	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[order],
		) {
		},
	)
	if err != nil {
		t.Fatalf(
			"Replay: %v",
			err,
		)
	}
}

func TestUpcast_ChainApplied(
	t *testing.T,
) {
	var calls []int
	upcasters := map[int]event.Upcaster{
		1: upcastStepRecorder{calls: &calls, step: 1}.upcast,

		2: upcastStepRecorder{calls: &calls, step: 2}.upcast,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		3,
	)

	evt := esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(
		context.Background(),
		evt,
	)
	if err != nil {
		t.Fatalf(
			"upcastInternalEvent: %v",
			err,
		)
	}

	if upcasted.SchemaVersion != 3 {
		t.Errorf(
			"SchemaVersion = %d, want 3",
			upcasted.SchemaVersion,
		)
	}

	if len(calls) != 2 ||
		calls[0] != 1 ||
		calls[1] != 2 {
		t.Errorf(
			"chain calls = %v, want [1 2]",
			calls,
		)
	}
}

func TestUpcast_MissingUpcasterSkipped(
	t *testing.T,
) {
	upcasters := map[int]event.Upcaster{
		2: keepPayloadUpcaster,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		3,
	)

	evt := esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)}
	upcasted, err := r.upcastInternalEvent(
		context.Background(),
		evt,
	)
	if err != nil {
		t.Fatalf(
			"upcastInternalEvent: %v",
			err,
		)
	}

	if upcasted.SchemaVersion != 3 {
		t.Errorf(
			"SchemaVersion = %d, want 3",
			upcasted.SchemaVersion,
		)
	}
}

func TestUpcast_ErrorWrapped(
	t *testing.T,
) {
	inner := errors.New("inner")
	upcasters := map[int]event.Upcaster{
		1: failingUpcaster{err: inner}.upcast,
	}
	r := newTestReplayer(
		store.New(),
		upcasters,
		2,
	)

	_, err := r.upcastInternalEvent(
		context.Background(),
		esmodels.InternalEvent{SchemaVersion: 1, Patches: json.RawMessage(`[]`)},
	)
	if err == nil {
		t.Fatal("expected error")
	}

	if !errors.Is(
		err,
		inner,
	) {
		t.Errorf(
			"error = %v, expected to wrap inner",
			err,
		)
	}
}

func TestApplyPatchesToJSON_Nil_NoOp(
	t *testing.T,
) {
	stateJSON := []byte(`{"Status":"Pending"}`)
	result, err := applyPatchesToJSON(
		stateJSON,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"applyPatchesToJSON(nil): %v",
			err,
		)
	}

	if string(result) != string(stateJSON) {
		t.Errorf(
			"got %s, want %s",
			result,
			stateJSON,
		)
	}
}

func TestApplyPatchesToJSON_Null_NoOp(
	t *testing.T,
) {
	stateJSON := []byte(`{"Status":"Pending"}`)
	result, err := applyPatchesToJSON(
		stateJSON,
		json.RawMessage("null"),
	)
	if err != nil {
		t.Fatalf(
			"applyPatchesToJSON(null): %v",
			err,
		)
	}

	if string(result) != string(stateJSON) {
		t.Errorf(
			"got %s, want %s",
			result,
			stateJSON,
		)
	}
}

func TestApplyPatchesToJSON_Empty_NoOp(
	t *testing.T,
) {
	stateJSON := []byte(`{"Status":"Pending"}`)
	result, err := applyPatchesToJSON(
		stateJSON,
		json.RawMessage(`[]`),
	)
	if err != nil {
		t.Fatalf(
			"applyPatchesToJSON([]): %v",
			err,
		)
	}

	if string(result) != string(stateJSON) {
		t.Errorf(
			"got %s, want %s",
			result,
			stateJSON,
		)
	}
}

func TestApplyPatchesToJSON_InvalidPatch(
	t *testing.T,
) {
	_, err := applyPatchesToJSON(
		[]byte(`{"Status":"Pending"}`),
		json.RawMessage(`not-json`),
	)
	if err == nil {
		t.Fatal("expected error on invalid patch JSON")
	}
}

func TestApplyPatchesToJSON_ApplyError(
	t *testing.T,
) {
	_, err := applyPatchesToJSON(
		[]byte(`{"Status":"Pending"}`),
		json.RawMessage(`[{"op":"test","path":"/Status","value":"wrong"}]`),
	)
	if err == nil {
		t.Fatal("expected error from failing test op")
	}
}

func TestApplyPatchesToJSON_Success(
	t *testing.T,
) {
	result, err := applyPatchesToJSON(
		[]byte(`{"ID":"1","Status":"Pending","Total":0}`),
		json.RawMessage(`[{"op":"replace","path":"/Status","value":"Shipped"}]`),
	)
	if err != nil {
		t.Fatalf(
			"applyPatchesToJSON: %v",
			err,
		)
	}

	var got order
	if err := json.Unmarshal(
		result,
		&got,
	); err != nil {
		t.Fatalf(
			"unmarshal result: %v",
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

func TestHydrate_PatchApplyError(
	t *testing.T,
) {
	r := newTestReplayer(
		store.New(),
		nil,
		1,
	)

	events := []esmodels.InternalEvent{
		{ID: "e1", Version: 1, SchemaVersion: 1,
			Patches: json.RawMessage(`[{"op":"replace","path":"/NonExistent","value":"x"}]`)},
	}

	_, err := r.Hydrate(
		context.Background(),
		order{},
		events,
	)
	if err == nil {
		t.Fatal("expected error from failed patch application in Hydrate")
	}
}

func TestReplay_UpcasterError(
	t *testing.T,
) {
	upcastErr := errors.New("upcaster broken")
	upcasters := map[int]event.Upcaster{
		1: failingUpcaster{err: upcastErr}.upcast,
	}
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

	r := newTestReplayer(
		es,
		upcasters,
		2,
	)

	err := r.Replay(
		ctx,
		"agg1",
		1,
		0,
		new(eventCollector).collect,
	)
	if !errors.Is(
		err,
		upcastErr,
	) {
		t.Errorf(
			"err = %v, want upcastErr",
			err,
		)
	}
}

func TestReplay_PatchApplyError(
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
			json.RawMessage(`[{"op":"replace","path":"/NonExistent","value":"x"}]`),
		),
	)

	r := newTestReplayer(
		es,
		nil,
		1,
	)

	err := r.Replay(
		ctx,
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[order],
		) {
		},
	)
	if err == nil {
		t.Fatal("expected error from failed patch application in Replay")
	}
}

func TestReplay_MarshalStateZeroValueError(
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

	r := New[mocks.ErrMarshal](
		es,
		make(map[int]event.Upcaster),
		1,
		mocks.ErrMarshal{},
	)

	err := r.Replay(
		ctx,
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[mocks.ErrMarshal],
		) {
		},
	)
	if err == nil {
		t.Fatal("expected marshal error on stateZeroValue")
	}
}

func TestReplay_UnmarshalCurrentError(
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

	r := New[mocks.BadUnmarshal](
		es,
		make(map[int]event.Upcaster),
		1,
		mocks.BadUnmarshal{},
	)

	err := r.Replay(
		ctx,
		"agg1",
		1,
		0,
		func(
			ctx context.Context,
			e event.Event[mocks.BadUnmarshal],
		) {
		},
	)
	if err == nil {
		t.Fatal("expected unmarshal error in Replay loop")
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

func populateEvents(
	b *testing.B,
	es store.Memory,
	n int,
) {
	b.Helper()

	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	for i := 1; i <= n; i++ {
		appendBenchEvent(
			b,
			es,
			int64(i),
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

func benchEventCounts() []int {
	return []int{1, 10, 10_000}
}

func BenchmarkHydrate(
	b *testing.B,
) {
	for _, n := range benchEventCounts() {
		b.Run(
			"events="+groupedCount(n),
			hydrateBench(n),
		)
	}
}

func hydrateBench(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		events := benchInternalEvents(n)
		r := newTestReplayer(
			store.New(),
			nil,
			1,
		)
		benchHydrateLoop(
			b,
			r,
			events,
		)
	}
}

func benchHydrateLoop(
	b *testing.B,
	r *Replayer[order],
	events []esmodels.InternalEvent,
) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustHydrate(
			b,
			r,
			events,
		)
	}
}

func benchInternalEvents(
	n int,
) []esmodels.InternalEvent {
	patch := json.RawMessage(`[{"op":"replace","path":"/Status","value":"x"}]`)
	now := time.Now().UTC()
	events := make(
		[]esmodels.InternalEvent,
		n,
	)
	for i := range events {
		events[i] = esmodels.InternalEvent{
			ID:            "bench",
			EventName:     "Updated",
			Version:       int64(i + 1),
			SchemaVersion: 1,
			OccurredAt:    now,
			Patches:       patch,
		}
	}

	return events
}

func mustHydrate(
	b *testing.B,
	r *Replayer[order],
	events []esmodels.InternalEvent,
) {
	b.Helper()

	_, err := r.Hydrate(
		context.Background(),
		order{},
		events,
	)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkReplay(
	b *testing.B,
) {
	for _, n := range benchEventCounts() {
		b.Run(
			"events="+groupedCount(n),
			replayBench(n),
		)
	}
}

func replayBench(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		es := store.New()
		populateEvents(
			b,
			es,
			n,
		)
		r := newTestReplayer(
			es,
			nil,
			1,
		)
		benchReplayLoop(
			b,
			r,
		)
	}
}

func benchReplayLoop(
	b *testing.B,
	r *Replayer[order],
) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mustReplay(
			b,
			r,
		)
	}
}

func mustReplay(
	b *testing.B,
	r *Replayer[order],
) {
	b.Helper()

	noopHandler := func(
		_ context.Context,
		_ event.Event[order],
	) {
	}
	err := r.Replay(
		context.Background(),
		"agg1",
		1,
		0,
		noopHandler,
	)
	if err != nil {
		b.Fatal(err)
	}
}

func groupedCount(
	n int,
) string {
	if n < 1000 {
		return strconv.Itoa(n)
	}

	return groupedCount(n/1000) +
		fmt.Sprintf(
			"_%03d",
			n%1000,
		)
}

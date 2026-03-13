package replayer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
	jsonpatch "github.com/evanphx/json-patch/v5"
)

// Replayer applies patches sequentially to reconstruct aggregate state,
// upcasting events to the current schema version as needed.
//
// Hydrate is the core method, called by Reader for both warm and cold paths.
// Replay is the public read-only method for manual recovery and projections.
type Replayer[T any] struct {
	eventStore           asynxmd.Store
	upcasters            map[int]asynxmd.Upcaster
	currentSchemaVersion int
	stateZeroValue       T
}

// New constructs a Replayer. upcasters maps SchemaVersion → migration func;
// schemaVersion is the current (target) schema version. upcasters is cloned
// so callers may safely mutate the original map after construction.
func New[T any](
	es asynxmd.Store,
	upcasters map[int]asynxmd.Upcaster,
	schemaVersion int,
	zeroValue T,
) *Replayer[T] {
	cloned := maps.Clone(upcasters)
	if cloned == nil {
		cloned = make(map[int]asynxmd.Upcaster)
	}
	return &Replayer[T]{
		eventStore:           es,
		upcasters:            cloned,
		currentSchemaVersion: schemaVersion,
		stateZeroValue:       zeroValue,
	}
}

// HydrateResult is returned by Hydrate. When DidUpcast is true, LastVersion
// holds the version at which the state was sealed — the caller should write
// an auto-snapshot using State and LastVersion.
type HydrateResult[T any] struct {
	State       T
	DidUpcast   bool
	LastVersion int64
}

// Hydrate applies a sequence of InternalEvents to seedState and returns
// the final aggregate state. When any event required upcasting, the result
// signals this via DidUpcast so the caller can write an auto-snapshot.
//
// Hydrate is read-only: it never writes to any store.
func (r *Replayer[T]) Hydrate(
	ctx context.Context,
	aggregateID string,
	seedState T,
	events []esmodels.InternalEvent,
) (HydrateResult[T], error) {
	if len(events) == 0 {
		return HydrateResult[T]{State: seedState}, nil
	}

	stateJSON, err := json.Marshal(seedState)
	if err != nil {
		return HydrateResult[T]{}, err
	}

	upcasted := false
	var lastVersion int64

	for _, evt := range events {
		if evt.SchemaVersion < r.currentSchemaVersion {
			upcasted = true
		}

		upcastedEvt, err := r.upcastInternalEvent(ctx, evt)
		if err != nil {
			return HydrateResult[T]{}, err
		}

		stateJSON, err = applyPatchesToJSON(stateJSON, upcastedEvt.Patches)
		if err != nil {
			return HydrateResult[T]{}, err
		}

		lastVersion = evt.Version
	}

	var current T
	if err := json.Unmarshal(stateJSON, &current); err != nil {
		return HydrateResult[T]{}, err
	}

	result := HydrateResult[T]{State: current}
	if upcasted {
		result.DidUpcast = true
		result.LastVersion = lastVersion
	}

	return result, nil
}

// Replay iterates events in version order, upcasting each to the current schema
// version, and calls fn with the public Event[T] (including full state and
// previous state). Replay is read-only and never writes auto-snapshots.
//
// toVersion=0 loads all events from fromVersion to the end of the stream.
func (r *Replayer[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn func(asynxmd.Event[T]),
) error {
	var (
		eventBlobs [][]byte
		err        error
	)

	if toVersion == 0 {
		eventBlobs, err = r.eventStore.ReadFrom(ctx, "events:"+aggregateID, fromVersion)
	} else {
		count := toVersion - fromVersion + 1
		eventBlobs, err = r.eventStore.ReadRange(ctx, "events:"+aggregateID, fromVersion, count)
	}

	if err != nil {
		return err
	}

	if len(eventBlobs) == 0 {
		return nil
	}

	currentJSON, err := json.Marshal(r.stateZeroValue)
	if err != nil {
		return err
	}

	previous := r.stateZeroValue
	for _, blob := range eventBlobs {
		var evt esmodels.InternalEvent
		if err := json.Unmarshal(blob, &evt); err != nil {
			return err
		}

		upcastedEvt, err := r.upcastInternalEvent(ctx, evt)
		if err != nil {
			return err
		}

		currentJSON, err = applyPatchesToJSON(currentJSON, upcastedEvt.Patches)
		if err != nil {
			return err
		}

		var current T
		if err := json.Unmarshal(currentJSON, &current); err != nil {
			return err
		}

		fn(asynxmd.Event[T]{
			ID:                upcastedEvt.ID,
			AggregateID:       aggregateID,
			EventName:         upcastedEvt.EventName,
			Version:           upcastedEvt.Version,
			SchemaVersion:     upcastedEvt.SchemaVersion,
			OccurredAt:        upcastedEvt.OccurredAt,
			Aggregate:         current,
			PreviousAggregate: previous,
		})
		previous = current
	}

	return nil
}

// upcastInternalEvent applies the registered upcaster chain to bring evt from
// its SchemaVersion up to currentSchemaVersion. Each upcaster receives the
// JSON-encoded RFC 6902 patch array and must return a compatible replacement.
//
// If an upcaster returns an error, the chain fails immediately (fail fast).
// If an upcaster panics, the panic propagates to the caller.
func (r *Replayer[T]) upcastInternalEvent(
	ctx context.Context,
	evt esmodels.InternalEvent,
) (esmodels.InternalEvent, error) {
	for v := evt.SchemaVersion; v < r.currentSchemaVersion; v++ {
		upcaster, ok := r.upcasters[v]
		if !ok {
			continue
		}

		upcastedPatchesJSON, err := upcaster(ctx, evt.EventName, evt.Patches)
		if err != nil {
			return esmodels.InternalEvent{}, fmt.Errorf("eventstore: upcaster %d failed: %w", v, err)
		}

		evt.Patches = upcastedPatchesJSON
		evt.SchemaVersion = v + 1
	}

	return evt, nil
}

// applyPatchesToJSON applies patches to raw JSON bytes and returns the patched bytes.
// A nil, "null", or "[]" patch set is a no-op.
func applyPatchesToJSON(stateJSON []byte, patches json.RawMessage) ([]byte, error) {
	if len(patches) == 0 || bytes.Equal(patches, []byte("null")) || bytes.Equal(patches, []byte("[]")) {
		return stateJSON, nil
	}
	patch, err := jsonpatch.DecodePatch(patches)
	if err != nil {
		return nil, err
	}
	return patch.Apply(stateJSON)
}

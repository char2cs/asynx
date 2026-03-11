package eventstore

import (
	"context"
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch/v5"
	asynxmd "github.com/char2cs/asynx/models"
)

// Replayer applies patches sequentially to reconstruct aggregate state,
// upcasting events to the current schema version as needed.
//
// hydrate is the core method, called by Reader for both warm and cold paths.
// Replay is the public read-only method for manual recovery and projections.
type Replayer[T any] struct {
	eventStore           asynxmd.Store
	snapshotStore        asynxmd.Store
	upcasters            map[int]asynxmd.Upcaster
	currentSchemaVersion int
	stateZeroValue       T
}

// hydrate applies a sequence of internalEvents to seedState and returns the
// final aggregate state. If any event required upcasting, it writes an
// auto-snapshot to seal the schema migration for future warm-path loads.
//
// Returns (finalState, error). When the error is a failed auto-snapshot write,
// finalState is still correct and durable — the caller decides how to handle.
func (r *Replayer[T]) hydrate(
	ctx context.Context,
	aggregateID string,
	seedState T,
	events []internalEvent,
) (T, error) {
	current := seedState
	upcasted := false
	var lastVersion int64

	for _, evt := range events {
		if evt.SchemaVersion < r.currentSchemaVersion {
			upcasted = true
		}

		upcastedEvt, err := r.upcastInternalEvent(ctx, evt)
		if err != nil {
			return r.stateZeroValue, err
		}

		current, err = r.applyPatches(current, upcastedEvt.Patches)
		if err != nil {
			return r.stateZeroValue, err
		}

		lastVersion = evt.Version
	}

	// Seal the schema migration by writing a snapshot so subsequent
	// loads use the fast warm path instead of replaying old events again.
	if upcasted && len(events) > 0 {
		stateBytes, err := json.Marshal(current)
		if err != nil {
			return current, err
		}

		snap := snapshotBlob{
			Version:       lastVersion,
			SchemaVersion: r.currentSchemaVersion,
			State:         stateBytes,
		}

		snapJSON, err := json.Marshal(snap)
		if err != nil {
			return current, err
		}

		if err := r.snapshotStore.Append(ctx, "snapshots:"+aggregateID, lastVersion, snapJSON); err != nil {
			// Auto-snapshot failed — state is correct and durable.
			// Propagate so the caller can decide whether to retry.
			return current, err
		}
	}

	return current, nil
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

	if toVersion <= 0 {
		eventBlobs, err = r.eventStore.ReadFrom(ctx, "events:"+aggregateID, fromVersion)
	} else {
		count := toVersion - fromVersion + 1
		eventBlobs, err = r.eventStore.ReadRange(ctx, "events:"+aggregateID, fromVersion, count)
	}

	if err != nil {
		return err
	}

	current := r.stateZeroValue
	for _, blob := range eventBlobs {
		var evt internalEvent
		if err := json.Unmarshal(blob, &evt); err != nil {
			return err
		}

		previous := current

		upcastedEvt, err := r.upcastInternalEvent(ctx, evt)
		if err != nil {
			return err
		}

		current, err = r.applyPatches(current, upcastedEvt.Patches)
		if err != nil {
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
	}

	return nil
}

// upcastInternalEvent applies the registered upcaster chain to bring evt from
// its SchemaVersion up to r.currentSchemaVersion. Each upcaster receives the
// JSON-encoded RFC 6902 patch array and must return a compatible replacement.
//
// If an upcaster returns an error, the chain fails immediately (fail fast).
// If an upcaster panics, the panic propagates to the caller.
func (r *Replayer[T]) upcastInternalEvent(
	ctx context.Context,
	evt internalEvent,
) (internalEvent, error) {
	for v := evt.SchemaVersion; v < r.currentSchemaVersion; v++ {
		upcaster, ok := r.upcasters[v]
		if !ok {
			continue
		}

		upcastedPatchesJSON, err := upcaster(ctx, evt.EventName, evt.Patches)
		if err != nil {
			return internalEvent{}, fmt.Errorf("eventstore: upcaster %d failed: %w", v, err)
		}

		evt.Patches = upcastedPatchesJSON
		evt.SchemaVersion = v + 1
	}

	return evt, nil
}

// applyPatches serializes state to JSON, applies the RFC 6902 patch set, and
// deserializes the result back to T. A nil or empty patch set is a no-op.
func (r *Replayer[T]) applyPatches(
	state T,
	patches json.RawMessage,
) (T, error) {
	if len(patches) == 0 || string(patches) == "null" || string(patches) == "[]" {
		return state, nil
	}

	patch, err := jsonpatch.DecodePatch(patches)
	if err != nil {
		return r.stateZeroValue, err
	}

	stateJSON, err := json.Marshal(state)
	if err != nil {
		return r.stateZeroValue, err
	}

	patchedJSON, err := patch.Apply(stateJSON)
	if err != nil {
		return r.stateZeroValue, err
	}

	var patchedState T
	if err := json.Unmarshal(patchedJSON, &patchedState); err != nil {
		return r.stateZeroValue, err
	}

	return patchedState, nil
}

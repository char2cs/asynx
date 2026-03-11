package eventstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wI2L/jsondiff"
	asynxmd "github.com/char2cs/asynx/models"
	asynxutils "github.com/char2cs/asynx/internal/utils"
)

// Writer computes RFC 6902 diffs between state transitions and durably appends
// them as internalEvents to the event stream. It optionally writes a snapshot
// when the command requests one.
//
// Writer is stateless and safe for concurrent use.
type Writer[T any] struct {
	eventStore           asynxmd.Store
	snapshotStore        asynxmd.Store
	currentSchemaVersion int
}

// Write computes the RFC 6902 diff from previousState to newState, serializes
// it as an internalEvent, and appends it to the event stream at version.
// A snapshot is written to the snapshot stream if shouldSnapshot is true.
//
// The event stream append is the save point: once it succeeds the event is
// durable even if the snapshot write fails.
//
// Returns the public Event[T] (for bus publish) or an error.
func (w *Writer[T]) Write(
	ctx context.Context,
	aggregateID string,
	eventName string,
	previousState T,
	newState T,
	version int64,
	shouldSnapshot bool,
) (asynxmd.Event[T], error) {
	oldJSON, err := json.Marshal(previousState)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	newJSON, err := json.Marshal(newState)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	patch, err := jsondiff.CompareJSON(oldJSON, newJSON)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	eventID := asynxutils.NewID()
	now := time.Now().UTC()

	evt := internalEvent{
		ID:            eventID,
		EventName:     eventName,
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		OccurredAt:    now,
		Patches:       json.RawMessage(patchJSON),
	}

	evtJSON, err := json.Marshal(evt)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	// SAVE POINT: event is durable after this succeeds.
	if err := w.eventStore.Append(ctx, "events:"+aggregateID, version, evtJSON); err != nil {
		return asynxmd.Event[T]{}, err
	}

	if shouldSnapshot {
		if err := w.writeSnapshot(ctx, aggregateID, version, newState); err != nil {
			return asynxmd.Event[T]{}, err
		}
	}

	return asynxmd.Event[T]{
		ID:                eventID,
		AggregateID:       aggregateID,
		EventName:         eventName,
		Version:           version,
		SchemaVersion:     w.currentSchemaVersion,
		OccurredAt:        now,
		Aggregate:         newState,
		PreviousAggregate: previousState,
	}, nil
}

func (w *Writer[T]) writeSnapshot(
	ctx context.Context,
	aggregateID string,
	version int64,
	state T,
) error {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return err
	}

	snap := snapshotBlob{
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		State:         stateBytes,
	}

	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	return w.snapshotStore.Append(ctx, "snapshots:"+aggregateID, version, snapJSON)
}

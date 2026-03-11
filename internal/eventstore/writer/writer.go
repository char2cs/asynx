package writer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wI2L/jsondiff"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxutils "github.com/char2cs/asynx/internal/utils"
	asynxmd "github.com/char2cs/asynx/models"
)

// Writer computes RFC 6902 diffs between state transitions and durably appends
// them as InternalEvents to the event stream. It optionally writes a snapshot
// when the command requests one.
//
// Writer is stateless and safe for concurrent use.
type Writer[T any] struct {
	EventStore           asynxmd.Store
	SnapshotStore        asynxmd.Store
	CurrentSchemaVersion int
}

// Write determines the next version from storage, computes the RFC 6902 diff
// from previousState to newState, serializes it as an InternalEvent, and
// appends it to the event stream. A snapshot is written if shouldSnapshot is
// true.
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
	shouldSnapshot bool,
) (asynxmd.Event[T], error) {
	version, err := w.nextVersion(ctx, aggregateID)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

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

	evt := esmodels.InternalEvent{
		ID:            eventID,
		EventName:     eventName,
		Version:       version,
		SchemaVersion: w.CurrentSchemaVersion,
		OccurredAt:    now,
		Patches:       json.RawMessage(patchJSON),
	}

	evtJSON, err := json.Marshal(evt)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}

	// SAVE POINT: event is durable after this succeeds.
	if err := w.EventStore.Append(ctx, "events:"+aggregateID, version, evtJSON); err != nil {
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
		SchemaVersion:     w.CurrentSchemaVersion,
		OccurredAt:        now,
		Aggregate:         newState,
		PreviousAggregate: previousState,
	}, nil
}

// nextVersion determines the version at which the next event should be written.
// It reads the snapshot (to skip over sealed history) then counts delta events
// to compute latestVersion + 1.
//
// For a brand-new aggregate (no snapshot, no events) it returns 1.
func (w *Writer[T]) nextVersion(ctx context.Context, aggregateID string) (int64, error) {
	var snapVersion int64

	snapBlobs, err := w.SnapshotStore.ReadFrom(ctx, "snapshots:"+aggregateID, 0)
	if err != nil {
		return 0, err
	}

	if len(snapBlobs) > 0 {
		var snap esmodels.SnapshotBlob
		if err := json.Unmarshal(snapBlobs[len(snapBlobs)-1], &snap); err == nil {
			snapVersion = snap.Version
		}
	}

	deltaBlobs, err := w.EventStore.ReadFrom(ctx, "events:"+aggregateID, snapVersion+1)
	if err != nil {
		return 0, err
	}

	// Versions are consecutive integers starting at 1, so the next version is
	// simply (last known version) + 1 without unmarshalling individual blobs.
	return snapVersion + int64(len(deltaBlobs)) + 1, nil
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

	snap := esmodels.SnapshotBlob{
		Version:       version,
		SchemaVersion: w.CurrentSchemaVersion,
		State:         stateBytes,
	}

	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}

	return w.SnapshotStore.Append(ctx, "snapshots:"+aggregateID, version, snapJSON)
}

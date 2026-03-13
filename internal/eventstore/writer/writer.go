package writer

import (
	"context"
	"encoding/json"
	"time"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxutils "github.com/char2cs/asynx/internal/utils"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/wI2L/jsondiff"
)

// Writer computes RFC 6902 diffs between state transitions and durably appends
// them as InternalEvents to the event stream. It optionally writes a snapshot
// when the command requests one.
//
// Writer is stateless and safe for concurrent use.
type Writer[T any] struct {
	eventStore           asynxmd.Store
	snapshotStore        asynxmd.Store
	currentSchemaVersion int
}

// New constructs a Writer.
func New[T any](es, ss asynxmd.Store, schemaVersion int) *Writer[T] {
	return &Writer[T]{
		eventStore:           es,
		snapshotStore:        ss,
		currentSchemaVersion: schemaVersion,
	}
}

// Write determines the next version from storage, computes the RFC 6902 diff
// from previousState to newState, serializes it as an InternalEvent, and
// appends it to the event stream. A snapshot is written if shouldSnapshot is
// true.
//
// The event stream append is the save point: once it succeeds, the event is
// durable in the store. If snapshot write fails, the append is not rolled back
// (snapshot failures don't prevent durability of the event), but the error is
// still returned to the caller.
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

	patch, err := jsondiff.Compare(previousState, newState)
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
		SchemaVersion: w.currentSchemaVersion,
		OccurredAt:    now,
		Patches:       json.RawMessage(patchJSON),
	}

	evtJSON, err := json.Marshal(evt)
	if err != nil {
		return asynxmd.Event[T]{}, err
	}
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

// nextVersion determines the version at which the next event should be written.
// It reads the snapshot (to skip over sealed history) then counts delta events
// to compute latestVersion + 1.
//
// For a brand-new aggregate (no snapshot, no events) it returns 1.
func (w *Writer[T]) nextVersion(ctx context.Context, aggregateID string) (int64, error) {
	var snapVersion int64

	snapBlobs, err := w.snapshotStore.ReadFrom(ctx, "snapshots:"+aggregateID, 0)
	if err != nil {
		return 0, err
	}

	if len(snapBlobs) > 0 {
		// Only the version field is needed here — use a minimal struct so a
		// corrupt or schema-mismatched State does not block version recovery.
		var meta struct {
			Version int64 `json:"version"`
		}
		if err := json.Unmarshal(snapBlobs[len(snapBlobs)-1], &meta); err == nil {
			snapVersion = meta.Version
		}
	}

	count, err := w.eventStore.Count(ctx, "events:"+aggregateID, snapVersion+1)
	if err != nil {
		return 0, err
	}

	// Versions are consecutive integers starting at 1, so the next version is
	// simply (last known version) + 1 without unmarshalling individual blobs.
	return snapVersion + count + 1, nil
}

func (w *Writer[T]) writeSnapshot(ctx context.Context, aggregateID string, version int64, state T) error {
	snap := esmodels.SnapshotBlob[T]{
		Version:       version,
		SchemaVersion: w.currentSchemaVersion,
		State:         state,
	}
	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return w.snapshotStore.Append(ctx, "snapshots:"+aggregateID, version, snapJSON)
}

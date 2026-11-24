package replayer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"maps"
	"slices"
)

// Replayer applies patches sequentially to reconstruct aggregate state,
// upcasting events to the current schema version as needed.
//
// Hydrate is the core method, called by Reader for both warm and cold paths: it
// folds events already in memory into a state and reports whether any needed an
// upcast. Replay is the read-only method for manual recovery and projections:
// it reads the stream itself and hands each event to a callback. Neither ever
// writes to a store, which is why snapshotting is Reader's job. Replayer is
// immutable after New and safe for concurrent use.
type Replayer[T any] struct {
	eventStore           asynxmd.Store
	upcasters            map[int]asynxmd.Upcaster
	currentSchemaVersion int
	stateZeroValue       T
}

// New constructs a Replayer. upcasters maps SchemaVersion to the migration that
// lifts an event from that version to the next; schemaVersion is the current
// (target) schema version; zeroValue is the state a replay starts from.
// upcasters is cloned so callers may safely mutate the original map after
// construction; a nil map means no migrations, and a version without an entry
// is stepped over without changing the event.
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

// Hydrate applies a sequence of InternalEvents to seedState and returns
// the final aggregate state. When any event required upcasting, the result
// signals this via DidUpcast so the caller can write an auto-snapshot.
//
// Hydrate is read-only: it never writes to any store.
func (r *Replayer[T]) Hydrate(
	ctx context.Context,
	seedState T,
	events []esmodels.InternalEvent,
) (HydrateResult[T], error) {
	if len(events) == 0 {
		return HydrateResult[T]{State: seedState},
			nil
	}

	current, err := r.fold(
		ctx,
		seedState,
		events,
	)
	if err != nil {
		return HydrateResult[T]{},
			err
	}

	result := HydrateResult[T]{State: current}
	if slices.ContainsFunc(
		events,
		r.needsUpcast,
	) {
		result.DidUpcast = true
		result.LastVersion = events[len(events)-1].Version
	}

	return result,
		nil
}

func (r *Replayer[T]) fold(
	ctx context.Context,
	seedState T,
	events []esmodels.InternalEvent,
) (current T, err error) {
	stateJSON, err := json.Marshal(seedState)
	if err != nil {
		return current,
			err
	}

	stateJSON, err = r.applyEvents(
		ctx,
		stateJSON,
		events,
	)
	if err != nil {
		return current,
			err
	}

	err = json.Unmarshal(
		stateJSON,
		&current,
	)
	return current,
		err
}

func (r *Replayer[T]) needsUpcast(
	evt esmodels.InternalEvent,
) bool {
	return evt.SchemaVersion < r.currentSchemaVersion
}

func (r *Replayer[T]) applyEvents(
	ctx context.Context,
	stateJSON []byte,
	events []esmodels.InternalEvent,
) (out []byte, err error) {
	out = stateJSON
	for i := 0; i < len(events) &&
		err == nil; i++ {
		out, err = r.applyEvent(
			ctx,
			out,
			events[i],
		)
	}

	return out,
		err
}

func (r *Replayer[T]) applyEvent(
	ctx context.Context,
	stateJSON []byte,
	evt esmodels.InternalEvent,
) ([]byte, error) {
	upcastedEvt, err := r.upcastInternalEvent(
		ctx,
		evt,
	)
	if err != nil {
		return nil,
			err
	}

	return applyPatchesToJSON(
		stateJSON,
		upcastedEvt.Patches,
	)
}

// Replay iterates events in version order, upcasting each to the current schema
// version, and calls fn with the public Event[T] (including full state and
// previous state). Replay is read-only and never writes auto-snapshots.
//
// To ensure PreviousAggregate is correct for the first event, Replay always
// loads from version 1. When fromVersion > 1, earlier events are replayed
// to populate initial state but fn is not called for them.
//
// toVersion=0 loads all events from fromVersion to the end of the stream.
func (r *Replayer[T]) Replay(
	ctx context.Context,
	aggregateID string,
	fromVersion int64,
	toVersion int64,
	fn asynxmd.ProjectionHandler[T],
) error {
	eventBlobs, frame, err := r.replayStart(
		ctx,
		aggregateID,
	)
	if err != nil {
		return err
	}

	limit := replayLimit(
		len(eventBlobs),
		toVersion,
	)
	for i := 0; i < limit &&
		err == nil; i++ {
		frame, err = r.replayOne(
			ctx,
			aggregateID,
			eventBlobs[i],
			int64(i)+1,
			frame,
			bounds{
				from: fromVersion,
				to:   toVersion,
			},
			fn,
		)
	}

	return err
}

func (r *Replayer[T]) replayStart(
	ctx context.Context,
	aggregateID string,
) (eventBlobs [][]byte, frame replayFrame[T], err error) {
	eventBlobs, err = r.eventStore.ReadFrom(
		ctx,
		"events:"+aggregateID,
		1,
	)
	if err != nil ||
		len(eventBlobs) == 0 {
		return nil,
			frame,
			err
	}

	frame.State = r.stateZeroValue
	frame.JSON, err = json.Marshal(r.stateZeroValue)
	return eventBlobs,
		frame,
		err
}

func replayLimit(
	blobCount int,
	toVersion int64,
) int {
	if toVersion > 0 &&
		toVersion < int64(blobCount) {
		return int(toVersion)
	}

	return blobCount
}

func (r *Replayer[T]) replayOne(
	ctx context.Context,
	aggregateID string,
	blob []byte,
	version int64,
	previous replayFrame[T],
	window bounds,
	fn asynxmd.ProjectionHandler[T],
) (replayFrame[T], error) {
	next, err := r.advance(
		ctx,
		previous,
		blob,
	)
	if err != nil {
		return replayFrame[T]{},
			err
	}

	if !window.contains(version) {
		return next,
			nil
	}

	fn(
		ctx,
		asynxmd.Event[T]{
			ID:                next.Event.ID,
			AggregateID:       aggregateID,
			EventName:         next.Event.EventName,
			Version:           next.Event.Version,
			SchemaVersion:     next.Event.SchemaVersion,
			OccurredAt:        next.Event.OccurredAt,
			Aggregate:         next.State,
			PreviousAggregate: previous.State,
		},
	)

	return next,
		nil
}

func (r *Replayer[T]) advance(
	ctx context.Context,
	previous replayFrame[T],
	blob []byte,
) (replayFrame[T], error) {
	var evt esmodels.InternalEvent
	err := json.Unmarshal(
		blob,
		&evt,
	)
	if err != nil {
		return replayFrame[T]{},
			err
	}

	upcastedEvt, err := r.upcastInternalEvent(
		ctx,
		evt,
	)
	if err != nil {
		return replayFrame[T]{},
			err
	}

	return nextFrame(
		previous,
		upcastedEvt,
	)
}

func nextFrame[T any](
	previous replayFrame[T],
	evt esmodels.InternalEvent,
) (frame replayFrame[T], err error) {
	frame.Event = evt
	frame.JSON, err = applyPatchesToJSON(
		previous.JSON,
		evt.Patches,
	)
	if err != nil {
		return replayFrame[T]{},
			err
	}

	err = json.Unmarshal(
		frame.JSON,
		&frame.State,
	)
	if err != nil {
		return replayFrame[T]{},
			err
	}

	return frame,
		nil
}

func (r *Replayer[T]) upcastInternalEvent(
	ctx context.Context,
	evt esmodels.InternalEvent,
) (out esmodels.InternalEvent, err error) {
	out = evt
	for v := evt.SchemaVersion; v < r.currentSchemaVersion &&
		err == nil; v++ {
		out, err = r.upcastStep(
			ctx,
			out,
			v,
		)
	}

	return out,
		err
}

func (r *Replayer[T]) upcastStep(
	ctx context.Context,
	evt esmodels.InternalEvent,
	version int,
) (esmodels.InternalEvent, error) {
	upcaster, ok := r.upcasters[version]
	if !ok {
		return evt,
			nil
	}

	upcastedPatchesJSON, err := upcaster(
		ctx,
		evt.EventName,
		evt.Patches,
	)
	if err != nil {
		return esmodels.InternalEvent{},
			fmt.Errorf(
				"eventstore: upcaster %d failed: %w",
				version,
				err,
			)
	}

	evt.Patches = upcastedPatchesJSON
	evt.SchemaVersion = version + 1
	return evt,
		nil
}

func applyPatchesToJSON(
	stateJSON []byte,
	patches json.RawMessage,
) ([]byte, error) {
	if len(patches) == 0 ||
		bytes.Equal(
			patches,
			[]byte("null"),
		) ||
		bytes.Equal(
			patches,
			[]byte("[]"),
		) {
		return stateJSON,
			nil
	}

	patch, err := jsonpatch.DecodePatch(patches)
	if err != nil {
		return nil,
			err
	}

	return patch.Apply(stateJSON)
}

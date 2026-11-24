package asynx

import (
	"context"
)

// AggregateReader reads the current state of aggregates. State is rebuilt from
// the aggregate's events, starting from its latest snapshot when one exists
// (the warm path) or from the first event otherwise (the cold path); a snapshot
// that cannot be decoded is reported to the corruption hook (see
// Builder.WithCorruptionHook) and the cold path is used instead.
//
// Reads do not take the aggregate's command lock, so they do not wait behind
// the commands being run for the aggregate.
type AggregateReader[T any] interface {
	// Get returns the current state of the aggregate. It returns
	// models.ErrNotFound if the aggregate has no events, and otherwise the
	// error of the store or of an upcaster. ctx bounds the reads.
	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)

	// Exists reports whether the aggregate has at least one event. It reads a
	// single event rather than rebuilding the state, so it is cheaper than Get.
	// An aggregate that does not exist is false with a nil error, not an error.
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)

	// Preload rebuilds the aggregate's state and discards it, so that the cold
	// path's cost is paid ahead of time and a snapshot is available to later
	// Get calls. An aggregate that does not exist yet is not an error. Any
	// other error of Get is returned.
	Preload(
		ctx context.Context,
		aggregateID string,
	) error
}

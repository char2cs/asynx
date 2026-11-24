package ports

import "context"

// Reading reads aggregate state.
type Reading[T any] interface {
	// Get returns the current aggregate state.
	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)
	// Exists reports whether the aggregate has at least one event.
	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)
	// Preload pays the cold-path cost offline by triggering a full Get.
	Preload(
		ctx context.Context,
		aggregateID string,
	) error
}

package asynx

import (
	"context"
)

// AggregateReader reads aggregate state.
type AggregateReader[T any] interface {
	Get(
		ctx context.Context,
		aggregateID string,
	) (T, error)

	Exists(
		ctx context.Context,
		aggregateID string,
	) (bool, error)

	Preload(
		ctx context.Context,
		aggregateID string,
	) error
}

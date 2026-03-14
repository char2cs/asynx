package models

import "context"

type Store interface {
	// Append enforces (aggregateID, version) uniqueness — the sole coordination
	// mechanism for correctness across concurrent writers.
	Append(
		ctx context.Context,
		aggregateID string,
		version int64,
		data []byte,
	) error

	ReadFrom(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
	) ([][]byte, error)

	ReadRange(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)

	// Count returns the number of entries with version >= fromVersion.
	Count(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
	) (int64, error)
}

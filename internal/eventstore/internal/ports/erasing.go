package ports

import "context"

// Erasing removes persisted aggregate data.
type Erasing interface {
	// DeleteSnapshot removes the aggregate's snapshot.
	DeleteSnapshot(
		ctx context.Context,
		aggregateID string,
	) error
	// Delete removes the aggregate's events and snapshot.
	Delete(
		ctx context.Context,
		aggregateID string,
	) error
}

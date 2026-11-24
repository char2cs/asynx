package ports

import "context"

// Erasing removes persisted aggregate data. Both operations are idempotent:
// erasing something that does not exist succeeds. Erasing is destructive and
// does not coordinate with running commands; the caller must make sure none is
// in flight for the aggregate (Forget fences them off before calling).
type Erasing interface {
	// DeleteSnapshot removes the aggregate's snapshot and nothing else, which
	// costs only a slower next read because state is rebuilt from events.
	DeleteSnapshot(
		ctx context.Context,
		aggregateID string,
	) error
	// Delete removes the aggregate's events and then its snapshot, leaving
	// nothing to read, so the aggregate restarts at version 1 if written again.
	// The events go first in their own transaction: when removing the snapshot
	// then fails, a stale snapshot may remain and the call can be repeated.
	Delete(
		ctx context.Context,
		aggregateID string,
	) error
}

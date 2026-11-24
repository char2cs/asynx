package ports

import "context"

// Erasing removes persisted aggregate data held by the snapshot store. Erasing
// is idempotent: erasing something that does not exist succeeds. It does not
// coordinate with running commands; the caller must make sure none is in flight
// for the aggregate (Forget fences them off before calling). Erasing the event
// stream is not part of this interface: Forget deletes it inside its own
// transaction on the store that Inspecting.Store returns.
type Erasing interface {
	// DeleteSnapshot removes the aggregate's snapshot and nothing else, which
	// costs only a slower next read because state is rebuilt from events.
	DeleteSnapshot(
		ctx context.Context,
		aggregateID string,
	) error
}

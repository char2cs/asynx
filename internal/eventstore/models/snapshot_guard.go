package models

// SnapshotGuard runs fn while no command or Forget can run for the aggregate,
// and reports whether it did. It is how a lock-free read gets to write its
// auto-snapshot without racing a Forget's erase.
type SnapshotGuard func(
	aggregateID string,
	fn func(),
) bool

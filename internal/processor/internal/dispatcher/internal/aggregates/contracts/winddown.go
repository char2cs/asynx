package contracts

// Winddown ends the workers of aggregates and closes the registry.
type Winddown interface {
	// Retire ends the aggregate's worker and reports whether it may exit. It is
	// false, and nothing changes, while jobs are queued: a job enqueued under
	// the mutex keeps the worker alive. Otherwise the worker is released (the
	// next Enqueue spawns a new one) and the record itself is removed unless
	// it holds a park or undelivered versions, which survive the retirement.
	Retire(
		aggregateID string,
	) bool

	// Close makes every later Enqueue fail with ErrDispatcherClosed. It
	// reports whether this call closed the registry (false when it was
	// already closed).
	Close() bool
}

package contracts

// Winddown ends the turns of jobs and closes the registry.
type Winddown interface {
	// Finish ends the turn of the aggregate's job that holds done: it closes
	// done, which lets the aggregate's next job begin, and releases the job's
	// hold on the aggregate's record, which is removed unless it holds a park,
	// undelivered versions or other jobs. Every job Enqueue admitted is
	// finished exactly once, whether it ran or was dropped by a park.
	Finish(
		aggregateID string,
		done chan<- struct{},
	)

	// Close makes every later Enqueue fail with ErrDispatcherClosed. It
	// reports whether this call closed the registry (false when it was
	// already closed).
	Close() bool
}

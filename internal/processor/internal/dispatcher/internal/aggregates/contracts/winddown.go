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

	// Close makes every later Enqueue fail with ErrDispatcherClosed. Jobs the
	// registry already admitted still run and finish; closing a closed
	// registry changes nothing.
	Close()
}

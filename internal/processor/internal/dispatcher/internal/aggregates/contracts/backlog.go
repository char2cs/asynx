package contracts

// Backlog is the FIFO of jobs queued for each aggregate.
type Backlog[J any] interface {
	// Enqueue appends job to its aggregate's FIFO and signals the aggregate's
	// wake channel. It never blocks on handlers and never fails for a full
	// queue.
	//
	// After Close it returns ErrDispatcherClosed and does nothing else.
	//
	// A durable job (see Item.Acked) for a parked aggregate is not queued:
	// its version is recorded as undelivered and it is marked unrun with
	// ErrDeliveryParked, both under the mutex, so a Lift cannot run between the
	// check and the record; then, after the mutex is released, it is completed
	// with ErrDeliveryParked, and Admission.Parked is true. Live-only jobs are
	// queued whatever the park.
	//
	// A queued job is first passed to Hooks.Admit. If the aggregate has no
	// worker, Enqueue gives it fresh wake and halt channels, calls Hooks.Spawn
	// and reports Admission.Spawned.
	Enqueue(
		job J,
	) (Admission, error)

	// Pop removes and returns the oldest job queued for the aggregate, together
	// with the halt channel it is delivered under. Reading both under one hold
	// of the mutex is what lets a park or supersede that follows be recognised
	// as concerning this job: a halt channel read later could already be the
	// next generation's. ok is false when nothing is queued.
	Pop(
		aggregateID string,
	) (job J, halt <-chan struct{}, ok bool)

	// Wake returns the channel (capacity 1) that Enqueue signals when it
	// queues a job for the aggregate. It is nil for an aggregate the registry
	// holds no record of. Only the aggregate's worker waits on it.
	Wake(
		aggregateID string,
	) <-chan struct{}
}

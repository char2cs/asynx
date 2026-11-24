package contracts

// Backlog is the FIFO of jobs queued for each aggregate, and the chain of
// turns that runs them one at a time.
type Backlog[J any] interface {
	// Enqueue appends job to its aggregate's FIFO and chains it behind the
	// aggregate's previous job: Admission.Turn closes when that one has
	// finished. It never blocks on handlers and never fails for a full queue.
	//
	// What became of the job is written into admission, in the same hold of
	// the mutex that decided it. After Close, admission.Err is
	// ErrDispatcherClosed and nothing else is done.
	//
	// A durable job (see Item.Acked) for a parked aggregate is not queued:
	// its version is recorded as undelivered and it is marked unrun with
	// ErrDeliveryParked, both under the mutex, so a Lift cannot run between the
	// check and the record; then, after the mutex is released, it is completed
	// with ErrDeliveryParked, and Admission.Parked is true. Live-only jobs are
	// queued whatever the park.
	//
	// A queued job is first passed to Hooks.Admit. The caller owes the job a
	// Begin once its Turn has closed, and then a Finish with its Done.
	Enqueue(
		job J,
		admission *Admission,
	)

	// Begin starts job's turn: it removes job from its aggregate's FIFO and
	// returns the halt channel it is delivered under. Reading both under one
	// hold of the mutex is what lets a park or supersede that follows be
	// recognised as concerning this job: a halt channel read later could
	// already be the next generation's. ok is false when a park dropped job
	// from the FIFO meanwhile: it has been settled and completed, and has
	// nothing left to run.
	Begin(
		job J,
	) (halt <-chan struct{}, ok bool)
}

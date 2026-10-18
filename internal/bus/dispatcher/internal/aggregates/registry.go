package aggregates

// Registry holds one record per aggregate under one mutex (see the package
// doc for the lock rule and the park state table). Each method is atomic: it
// takes the mutex once and changes the record entirely under that hold.
type Registry[J Item] interface {
	// Enqueue appends job to its aggregate's FIFO and signals the aggregate's
	// wake channel. It never blocks on handlers and never fails for a full
	// queue.
	//
	// After Close it returns ErrDispatcherClosed and does nothing else.
	//
	// A durable job (see Item.Durable) for a parked aggregate is not queued:
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
	// Retire ends the aggregate's worker and reports whether it may exit. It is
	// false, and nothing changes, while jobs are queued: a job enqueued under
	// the mutex keeps the worker alive. Otherwise the worker is released (the
	// next Enqueue spawns a new one) and the record itself is removed unless
	// it holds a park or undelivered versions, which survive the retirement.
	Retire(
		aggregateID string,
	) bool
	// Park halts the aggregate's durable delivery, as exhausting its attempts
	// does, and records the given versions (still pending in the delivery log)
	// as undelivered, so Unrun answers for them. On a running aggregate it
	// takes a new generation, closes the halt channel (a delivery in progress
	// gives up) and drops the queued durable jobs: each is marked unrun,
	// recorded and completed with ErrDeliveryParked under the mutex; live-only
	// jobs stay queued. On an already parked aggregate it only merges the
	// versions, keeping the generation. A record is created for an aggregate
	// the registry knew nothing of.
	Park(
		aggregateID string,
		undelivered ...int64,
	)
	// ParkIfCurrent parks the aggregate as Park does, unless durable is false
	// or halt (the channel Pop returned with the job) has been closed since:
	// then someone else has already settled the aggregate's fate (a Supersede
	// abandoned the delivery, a park was imposed and possibly lifted). It
	// reports whether halt was still open, so the park call took effect.
	ParkIfCurrent(
		aggregateID string,
		halt <-chan struct{},
		durable bool,
	) bool
	// SettleIfParked settles job as unrun with ErrDeliveryParked if its
	// aggregate is parked, or was parked since halt was taken, and reports
	// whether it did. Only a park still in force records the version as
	// undelivered: after a lift the redelivery covers the job, and a record
	// would outlive the park. The job is not completed.
	SettleIfParked(
		aggregateID string,
		halt <-chan struct{},
		job J,
	) bool
	// Lift lifts the park of the given generation and reports whether it did;
	// a park that is gone, or has been lifted and imposed again since the
	// caller listed it (see Parks), is left alone. It returns, in ascending
	// order, the versions the park left undelivered, and forgets them; the
	// halt channel is replaced by an open one.
	Lift(
		aggregateID string,
		generation uint64,
	) (lifted bool, undelivered []int64)
	// Supersede abandons the aggregate's pending durable deliveries ahead of an
	// erase: it parks the aggregate (dropping its queued durable jobs and
	// closing the halt channel, if it was running) and lifts the park at once,
	// so the aggregate is left running with no undelivered record.
	Supersede(
		aggregateID string,
	)
	// Parks returns a copy of the parked aggregates with the generation of each
	// park. A park that is lifted and imposed again has a new generation.
	Parks() map[string]uint64
	// Unrun reports why no handler ran for the event at (aggregateID, version):
	// ErrDeliveryParked while the aggregate is parked and the event was left
	// undelivered by it, nil otherwise.
	Unrun(
		aggregateID string,
		version int64,
	) error
	// Close makes every later Enqueue fail with ErrDispatcherClosed. It
	// reports whether this call closed the registry (false when it was
	// already closed).
	Close() bool
}

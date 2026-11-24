package contracts

// ParkSetter imposes parks on aggregates.
type ParkSetter[J any] interface {
	// Park halts the aggregate's durable delivery, as exhausting its attempts
	// does, and records the given versions (still pending in the delivery log)
	// as undelivered, so Unrun answers for them. On a running aggregate it
	// takes a new generation, closes the halt channel (a delivery in progress
	// gives up) and drops the queued durable jobs: each is marked unrun,
	// recorded and completed with ErrDeliveryParked under the mutex; live-only
	// jobs stay queued. On an already parked aggregate it only merges the
	// versions, keeping the generation. cause is recorded as the reason for
	// the park, unless the record already holds a non-nil cause: the first
	// non-nil cause is kept until the park lifts. A record is created for an
	// aggregate the registry knew nothing of.
	Park(
		aggregateID string,
		cause error,
		undelivered ...int64,
	)

	// ParkIfCurrent parks the aggregate for cause as Park does, unless halt
	// (the channel Pop returned with the job) has been closed since: then
	// someone else has already settled the aggregate's fate (a Supersede
	// abandoned the delivery, a park was imposed and possibly lifted). It
	// reports whether halt was still open, so the park call took effect.
	ParkIfCurrent(
		aggregateID string,
		halt <-chan struct{},
		cause error,
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
}

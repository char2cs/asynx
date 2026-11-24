package contracts

// ParkLifter lifts the parks of aggregates.
type ParkLifter interface {
	// Lift lifts the park of the given generation and reports whether it did;
	// a park that is gone, or has been lifted and imposed again since the
	// caller listed it (see Parks), is left alone. It writes, in ascending
	// order, the versions the park left undelivered, and forgets them; the
	// halt channel is replaced by an open one.
	Lift(
		aggregateID string,
		generation uint64,
		outcome *LiftOutcome,
	)

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
}

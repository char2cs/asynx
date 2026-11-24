package models

// DispatcherParkControl parks and supersedes the durable delivery of aggregates and lists the parks.
type DispatcherParkControl interface {
	// Park halts the aggregate's durable delivery, as exhausting its attempts
	// does: queued requests carrying an Ack leave memory and are recorded as
	// undelivered, a durable retry in progress gives up, and later requests
	// carrying an Ack are not queued until ResumeUndelivered lifts the park.
	// Queued requests without an Ack stay queued. The undelivered
	// versions, still pending in the delivery log, are added to the record, so
	// a retry of the command that produced one is told so (see Unrun). cause
	// says why, and Inspect reports it while the park lasts. Parking an
	// aggregate that is already parked keeps its generation, merges the
	// versions and keeps the first non-nil cause; a nil cause is replaced by
	// the cause of a later park.
	Park(
		aggregateID string,
		cause error,
		undelivered ...int64,
	)

	// Supersede abandons the aggregate's pending durable deliveries ahead of an
	// erase: queued ones leave memory, a retry in progress gives up, and the
	// aggregate is left unparked with no undelivered record. The entries stay
	// in the delivery log for the caller to drop. Interrupted handlers may
	// still be finishing.
	Supersede(
		aggregateID string,
	)

	// Parks lists the parked aggregates with the generation of each park. A
	// park that is lifted and imposed again has a new generation. The map is a
	// copy the caller owns.
	Parks() map[string]uint64
}

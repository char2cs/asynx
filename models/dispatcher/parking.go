package dispatcher

// Parking halts an aggregate's durable delivery and resumes it. A
// parked aggregate holds back every request that carries an Ack, so that no
// later event overtakes the one that could not be delivered, until a caller
// lifts the park.
type Parking interface {
	// Park halts the aggregate's durable delivery, as exhausting its attempts
	// does: queued requests carrying an Ack leave memory and are recorded as
	// undelivered, a durable retry in progress gives up, and later requests
	// carrying an Ack are not queued until ResumeUndelivered lifts the park.
	// Queued requests without an Ack stay queued. The undelivered versions,
	// still pending in the delivery log, are added to the record, so a retry
	// of the command that produced one is told so (see
	// dispatch.Inspector.Unrun). cause says why, and Inspect reports it while
	// the park lasts. Parking an aggregate that is already parked keeps its
	// generation, merges the versions and keeps the first non-nil cause; a nil
	// cause is replaced by the cause of a later park.
	Park(
		aggregateID string,
		cause error,
		undelivered ...int64,
	)

	// Parks lists the parked aggregates with the generation of each park. A
	// park that is lifted and imposed again has a new generation. The map is a
	// copy the caller owns.
	Parks() map[string]uint64

	// ResumeUndelivered lifts the park of the given generation and reports
	// whether it did. A park that is gone, or has been lifted and imposed again
	// since the caller listed it (see Parks), is left alone, because what the
	// caller read about it is stale. It also returns, in ascending order, the
	// versions the park left undelivered: the events no handler took, which a
	// redelivery must cover even when the caller's own scan of the delivery log
	// predates them. Putting them back if the redelivery fails is the caller's
	// (see Park), because lifting the park forgets them.
	ResumeUndelivered(
		aggregateID string,
		generation uint64,
	) (lifted bool, undelivered []int64)
}

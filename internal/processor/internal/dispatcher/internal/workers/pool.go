package workers

// Pool owns the per-aggregate worker goroutines.
//
// A worker handles each job it pops as follows. A job that carries an Ack and
// whose aggregate is parked, or was parked since the job was popped, is
// settled unrun and ends with ErrDeliveryParked without running anything.
// Otherwise the event is published to the live subscribers, unless the job
// skips them; a bus error, or a panic in the bus, is reported to the publish
// error handler and does not skip the durable delivery. A durable job then
// runs its durable delivery, and a job that carries an Ack is acknowledged
// once every durable subscriber has succeeded. A failed ack parks the
// aggregate before the error is reported to the publish error handler, so the
// aggregate's next event cannot overtake this one. A panic in the dispatcher
// itself never kills the worker: it is reported to the publish error handler
// and ends the job with that error. Whatever happens, the job is settled and
// completed when the worker is done with it.
type Pool[T any] interface {
	// Spawn starts the worker of aggregateID. The registry calls it, under its
	// mutex, when it queues a job for an aggregate without a worker; the
	// worker is counted before Spawn returns, so a Wait that follows a Close
	// of the registry also waits for it.
	Spawn(
		aggregateID string,
	)

	// Stop tells every worker to drain its queue and exit instead of waiting
	// for more work. Call it once, after the registry has been closed so no
	// new worker is spawned.
	Stop()

	// Wait blocks until every worker has exited: after Stop once they have
	// drained, otherwise as they retire after their idle timeout.
	Wait()
}

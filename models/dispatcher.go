package models

import "context"

// Dispatcher delivers committed events to the live and durable subscribers in
// per-aggregate order. The in-memory default is the only implementation asynx
// ships; another is built through a DispatcherFactory. The guarantees below
// are the contract every implementation carries:
//
//  1. Per aggregate, events are handled in Enqueue order, one at a time;
//     different aggregates are independent. Enqueue does not block on
//     handlers and never fails for a full queue.
//  2. DispatchEvent and DispatchTombstone requests reach the live subscribers
//     (Bus.PublishSync); DispatchRedelivery never does. A failing live
//     subscriber does not skip the durable delivery. A tombstone (and a
//     redelivered event named ForgetEventName) reaches every durable
//     subscriber; any other event, the matching ones.
//  3. A durable event runs its durable handlers with retry and backoff until
//     all succeed, the attempt cap is hit, or Interrupt is called; then Ack
//     runs, and only then the ticket's Final closes without error. The
//     aggregate's later events wait for it.
//  4. When the attempts of a request that carries an Ack run out, DeadLetter
//     may accept the event, which then counts as delivered; a deletion marker
//     (a DispatchTombstone, or a redelivered event named ForgetEventName) is
//     never dead-lettered. Otherwise the aggregate is parked; so it is when
//     Ack fails, on Interrupt while such a request is retrying, and on Park.
//     A request without an Ack whose attempts run out, or that is
//     interrupted, ends with an error but does not park the aggregate.
//  5. While an aggregate is parked, a request carrying an Ack is not queued:
//     its ticket settles with Unrun() == ErrDeliveryParked and ends with
//     ErrDeliveryParked, and its version is recorded as undelivered. Requests
//     without an Ack, live events and nil-Ack deletion markers alike, are
//     still queued and run.
//  6. ResumeUndelivered(aggregateID, generation) lifts only that generation's
//     park and returns the recorded undelivered versions; a stale generation
//     is a no-op. Park re-imposes a park with a new generation; on an already
//     parked aggregate it merges the versions into the record.
//  7. Supersede abandons the aggregate's queued and retrying durable
//     deliveries and leaves it unparked with no record.
//  8. TicketFor finds a ticket exactly while a delivery is queued, running or
//     retrying; Unrun answers for the events a park left undelivered, until
//     the park lifts.
//  9. InHandler(ctx, aggregateID) is true inside a handler this instance runs
//     for that aggregate; asynx uses it to refuse a SendWait or a Forget that
//     would wait on the handler itself.
//  10. Close waits for the workers to drain, honors ctx and may be retried;
//     Interrupt makes retries give up, parking the aggregate of a request
//     that carries an Ack; Interrupted closes once
//     either has been called; WaitIdle waits for what was in flight when it
//     was called.
//
// Delivery is at-least-once. The handlers in DispatcherConfig (dead-letter,
// publish, delivery and panic) must never take down a worker: a panic in one of
// them, or in a durable handler, is swallowed.
//
// An implementation is free to choose where handlers run, how events travel
// and persist while in flight, and how attempts are counted across restarts.
// It does not own the delivery log (the Store does), the aggregate lock, or the
// recovery orchestration: asynx reads the log itself and enqueues
// DispatchRedelivery requests.
type Dispatcher[T any] interface {
	// Enqueue puts the request on its aggregate's queue and returns its
	// ticket. Enqueue order is delivery order, so callers that must keep
	// versions ordered enqueue while holding their per-aggregate lock and wait
	// on the ticket afterwards. A request carrying an Ack for a parked
	// aggregate is not queued: its ticket settles and ends with
	// ErrDeliveryParked, and its version is recorded as undelivered (see
	// Unrun). After Close, Enqueue fails with ErrDispatcherClosed.
	Enqueue(
		ctx context.Context,
		request DispatchRequest[T],
	) (Ticket, error)

	// TicketFor returns the ticket of the event at (aggregateID, version) while
	// its delivery is under way: queued, being handled, or retrying. Once the
	// delivery is over, or if the event was never queued, there is none and
	// false is returned. A retried idempotent command uses it to wait for the
	// handlers of the event its first attempt produced.
	TicketFor(
		aggregateID string,
		version int64,
	) (Ticket, bool)

	// Unrun reports why no handler ran for the event at (aggregateID, version):
	// ErrDeliveryParked while the aggregate is parked and the event was left
	// undelivered by the park. It is nil for an event whose handlers ran,
	// whatever their outcome, and once the park has lifted.
	Unrun(
		aggregateID string,
		version int64,
	) error

	// Park halts the aggregate's durable delivery, as exhausting its attempts
	// does: queued requests carrying an Ack leave memory and are recorded as
	// undelivered, a durable retry in progress gives up, and later requests
	// carrying an Ack are not queued until ResumeUndelivered lifts the park.
	// Queued requests without an Ack stay queued. The undelivered
	// versions, still pending in the delivery log, are added to the record, so
	// a retry of the command that produced one is told so (see Unrun). Parking
	// an aggregate that is already parked keeps its generation and merges the
	// versions.
	Park(
		aggregateID string,
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

	// InHandler reports whether ctx belongs to a handler this dispatcher is
	// running for aggregateID. Waiting on that aggregate's deliveries from
	// there would wait on the handler itself.
	InHandler(
		ctx context.Context,
		aggregateID string,
	) bool

	// Interrupt makes failing durable deliveries give up retrying, leaving
	// them pending, without closing the dispatcher. The aggregate of a
	// delivery that carries an Ack is parked; one without an Ack is not. It is called at the start of shutdown so callers waiting on
	// a retrying event are released.
	Interrupt()

	// Interrupted is closed once Interrupt or Close has been called: from then
	// on failing durable deliveries give up instead of retrying.
	Interrupted() <-chan struct{}

	// Close stops accepting requests, interrupts retries and waits for every
	// worker to drain its queue. If ctx ends first the context error is
	// returned, the handlers' contexts are cancelled, and Close may be called
	// again to finish waiting.
	Close(
		ctx context.Context,
	) error

	// WaitIdle blocks until every event queued when it was called has been
	// handled. Events queued while it waits are not waited for, and are not
	// refused.
	WaitIdle()
}

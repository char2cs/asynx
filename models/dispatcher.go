package models

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
//     never dead-lettered. Otherwise the aggregate is parked, with
//     ErrDeliveryParked wrapping the last handler error as the cause; so it
//     is when Ack fails (the ack error), on Interrupt while such a request is
//     retrying (ErrDispatcherClosed), and on Park (the cause given).
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
//     parked aggregate it merges the versions into the record and keeps the
//     first non-nil cause. Inspect reports the park, its cause and its
//     undelivered versions; the cause is cleared when the park lifts.
//  7. Supersede abandons the aggregate's queued and retrying durable
//     deliveries and leaves it unparked with no record.
//  8. TicketFor finds a ticket while a delivery is queued, running or
//     retrying, and not for an event that was never queued; a finished
//     delivery may still be returned for a brief moment after its Final
//     closes, and then is not. Unrun answers for the events a park left
//     undelivered, until the park lifts.
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
//
// The interface is the union of three groups: DispatcherDelivery (queueing
// requests and looking at their deliveries), DispatcherParking (halting and
// resuming an aggregate's durable delivery) and DispatcherLifecycle
// (interrupting retries and shutting down). Every method is safe for
// concurrent use.
type Dispatcher[T any] interface {
	DispatcherDelivery[T]
	DispatcherParking
	DispatcherLifecycle
}

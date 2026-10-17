# Write-Path Guarantees Specification (v0.9.0)

## Overview

Asynx guarantees the following about every command, by construction rather than configuration. They hold for any `ShardingOpts` and any `WorkersPerShard`.

| # | Guarantee | Mechanism |
|---|-----------|-----------|
| 1 | **One writer per aggregate.** Commands for the same aggregate never run at the same time. | A per-aggregate lock held from load to enqueue. |
| 2 | **Ordered delivery.** Handlers see an aggregate's events in version order. | The event is enqueued to the per-aggregate dispatcher while the lock is held. |
| 3 | **One transaction.** The event, the synchronous read-model updates, the command receipt and the delivery entry commit together or not at all. | `Store.Atomically` is the only write path. |
| 4 | **Nothing durable is left undelivered, and never out of order.** Neither a crash after commit, a failing durable handler nor a shutdown mid-retry loses a delivery or lets a later event overtake an earlier one. | Delivery log, bounded retry with backoff, parking, recovery in `Build()`. |
| 5 | **Idempotent commands.** A retry of a command id is a no-op, and a rejection stays rejected. | Receipts written in the event's transaction. |
| 6 | **Fenced delete.** From the tombstone until the erase completes, no command for the aggregate can commit. | The aggregate is marked as being forgotten; commands are rejected with `ErrForgetInProgress`. |

Scope: **one process per store.** The lock and recovery are in-process. Several processes sharing a store stay free of corruption (the `(streamID, version)` uniqueness check still arbitrates, and a loser gets `ErrPipelineFailed`) but do not get exclusivity, cross-process ordering or coordinated recovery.

## Pipeline

For each command, under the aggregate's lock:

```
(receipt check)  → Load → Validate
  → Atomically{ Append → sync projections → PutReceipt → EnqueueDelivery* }
  → best-effort snapshot Put           (failure → SnapshotErrorHandler)
  → enqueue to the per-aggregate dispatcher
  → release the lock
```

`Send` returns here. `SendWait` then waits for the event's handlers **in the caller's goroutine**, not in a shard worker, so a slow or failing handler never occupies a worker, and the wait ends with the caller's context.

`*` only when durable subscribers are registered. The lock is released after the enqueue, not after `SendWait`'s handlers finish, so `SendWait` does not serialize an aggregate beyond its commit. Waiting for the lock respects the caller's context and returns `ErrContextCancelled`.

## Store Contract

```go
type Store interface {
    ReadFrom(ctx, streamID string, fromVersion int64) ([][]byte, error)
    ReadRange(ctx, streamID string, fromVersion, count int64) ([][]byte, error)
    GetReceipt(ctx, commandID string) (Receipt, bool, error)
    PendingDeliveries(ctx, limit int) ([]Delivery, error)   // enqueue order
    AckDelivery(ctx, aggregateID string, version int64) error
    Atomically(ctx, fn func(tx Tx) error) error
}

type Tx interface {
    Append(ctx, streamID string, version int64, data []byte) error
    DeleteStream(ctx, streamID string) error
    DropDeliveries(ctx, aggregateID string) error
    PutReceipt(ctx, commandID string, r Receipt) error      // duplicate → ErrDuplicateReceipt
    EnqueueDelivery(ctx, aggregateID string, version int64) error
    Native() any                                            // e.g. *sql.Tx
}
```

- `Atomically` commits when `fn` returns nil and rolls back everything done through `tx` when it returns an error. `fn` never calls `Atomically` again.
- `Append` enforces `(streamID, version)` uniqueness and returns an error wrapping `ErrPipelineFailed` on conflict.
- Stream IDs are `events:{aggregateID}` (unchanged from earlier versions, so existing data needs no migration). Delivery entries and receipts use raw aggregate IDs and command IDs.
- `PendingDeliveries` returns entries in enqueue order; for one aggregate they are in ascending version order (the lock guarantees it).
- `AckDelivery` and `DropDeliveries` are idempotent, and an ack of an entry that was dropped (or never existed) is not an error.
- `Forget` drops an aggregate's entries and enqueues the tombstone's entry in one transaction, and a forgotten aggregate restarts at version 1: a store must let `(aggregateID, version)` be enqueued again after the earlier entry was acknowledged or dropped (no soft-delete with a unique key), keep a drop-then-enqueue in one transaction ordered after other aggregates' entries, and restore dropped entries on rollback. The conformance suite checks all of these.
- `Native()` returns the backend's transaction handle so synchronous projections can write their own tables inside the same transaction. The in-memory store returns `nil`.
- Receipts and deliveries must live in the same database as the event streams, or `Atomically` cannot make them atomic.

### Conformance suite

`store/storetest` exports `Run(t, newStore)`, covering rollback, uniqueness, receipts, delivery order, acknowledgement, scoped deletes and `Native`. Run it from your adapter's tests:

```go
func TestConformance(t *testing.T) {
    storetest.Run(t, func(t *testing.T) models.Store { return newSQLiteStore(t) })
}
```

### Suggested SQL schema

```sql
CREATE TABLE events      (stream_id TEXT NOT NULL, version INTEGER NOT NULL, data BLOB NOT NULL,
                          PRIMARY KEY (stream_id, version));
CREATE TABLE receipts    (command_id TEXT PRIMARY KEY, accepted BOOLEAN NOT NULL,
                          aggregate_id TEXT, event_id TEXT, version INTEGER, rejection TEXT);
CREATE TABLE deliveries  (seq INTEGER PRIMARY KEY AUTOINCREMENT,
                          aggregate_id TEXT NOT NULL, version INTEGER NOT NULL,
                          UNIQUE (aggregate_id, version));
```

`PendingDeliveries` is `SELECT … ORDER BY seq LIMIT ?`.

## Synchronous Projections

```go
b.WithSyncProjection("OrderUpdated", func(ctx context.Context, tx models.Tx, evt models.Event[Order]) error {
    sqlTx := tx.Native().(*sql.Tx)
    _, err := sqlTx.ExecContext(ctx, `UPDATE order_view SET total=? WHERE id=?`, evt.Aggregate.Total, evt.AggregateID)
    return err
})
```

Projections match like `Subscribe` (exact event name, or a regular expression when the pattern has metacharacters) and run in registration order inside the append's transaction. An error rolls the transaction back: the event, receipt and delivery entry never existed, nothing is dispatched, and `Send` returns an error matching both `ErrPipelineFailed` and `ErrSyncProjection`. Write only through `tx.Native()`; a write through any other handle is not atomic. A failing projection blocks writes by design — that is the price of atomicity.

## Durable Delivery

```go
b.WithSubscriber("Order.*", func(ctx context.Context, evt models.Event[Order]) error {
    return readModel.Apply(ctx, evt)   // return an error to have the delivery retried
})
```

A durable handler returns an `error`. It is *not* registered on the bus: the dispatcher runs it itself so it can see failure.

- A delivery entry is written in the event's transaction. It is acknowledged only after **every** durable handler matching the event has returned nil. Only events at least one durable pattern matches get an entry.
- **Failure holds the line.** A handler that returns an error, panics, or fails after exceeding its timeout leaves the delivery pending. The dispatcher retries just the failed handlers with exponential backoff (default 100 ms doubling to 30 s; `WithDeliveryRetry`). That aggregate's later events wait behind it, so version 6 is never delivered before version 5 succeeded. Other aggregates are unaffected. Observe every failed attempt with `WithDeliveryErrorHandler(func(ctx, evt, err, attempt))`; a panic also reaches `WithPanicHandler`.
- **A subscriber is not isolated from its siblings.** Acknowledgement is per event, not per subscriber, so one failing durable subscriber holds back that aggregate's later events for *every* durable subscriber, and handlers that already succeeded see the event again on each retry (only failed handlers are retried within a streak, but after a restart all of them run again). If subscribers have unrelated failure modes, run them in separate `Asynx` instances over separate delivery logs, or make the slow one hand off to its own queue.
- **Retries stop: parking.** A delivery is attempted at most `WithDeliveryMaxAttempts(n)` times (default 20, about six minutes with the default backoff). Then:
  - if a dead-letter handler is set (`WithDeadLetterHandler`) and returns nil, the event is acknowledged as dealt with — store it somewhere for inspection — and the aggregate's later events flow;
  - otherwise the aggregate is **parked**: the failed event and every later one stay pending in the store, nothing more is delivered for that aggregate, and the delivery error handler receives an error matching `ErrDeliveryParked`. Other aggregates are unaffected. Commands for the aggregate still commit; their deliveries just wait in the log.
  - `ResumeDeliveries(ctx)` (or a restart; concurrent calls are safe, only the first redelivers) lifts the park and redelivers the pending events in order, under the aggregate's lock so no live event overtakes them. Call it once the handler or its dependency is fixed. Parking never lifts by itself.
- **Parking is visible.** `PausedAggregates()` lists the parked aggregates, and `SendWait` on one returns the committed event with an error matching `ErrDeliveryParked`, because no handler ran. Without a delivery error handler these two are the only signals, so poll `PausedAggregates` (or set the handler).
- **Live subscribers do not replay.** `Subscribe` and `Listen` are fire-and-forget. While an aggregate is parked its live-only events still flow, and a durable event that arrives meanwhile is not shown to live subscribers at all. `ResumeDeliveries` and the recovery at `Build` redeliver to durable subscribers only, so a live stream is never repeated or sent out of order; it just misses what was committed while parked or while the process was down.
- **The queue is not bounded.** Later events of an aggregate wait in an in-memory FIFO behind a slow or retrying delivery, so memory grows with the commands sent meanwhile. Retries are capped and parking empties the queue of durable events, which keeps a stuck delivery from holding it for long; a merely slow handler is never parked for being slow.
- **Shutdown is complete and retryable.** `Shutdown` stops accepting `Send`, `SendWait` and `Forget`, waits for those in flight (so none is stranded in a queue nobody reads), drains the workers, then closes the dispatcher and the bus. If its context ends first it returns that error and may be called again to finish; once it has completed, or while another call runs, it returns `ErrAlreadyShuttingDown`.
- **Shutdown never reorders.** `Shutdown` first interrupts every retry, so callers waiting on a failing delivery are released and the shutdown completes. An interrupted delivery parks its aggregate, so later events of that aggregate drain without being delivered or acknowledged; after a restart they are redelivered, v1 before v2.
- **A failing live bus does not skip durable delivery.** An error from the in-memory/custom bus's `PublishSync` is reported to `WithPublishErrorHandler`, and the durable handlers still run for that event.
- **Timeouts are alarms.** `WithHandlerTimeout` reports a slow handler (`ErrHandlerTimeout` to the delivery error handler; the timeout handler for bus subscribers) but the handler is still waited for, so the aggregate's next event never overlaps it and a delivery is never acknowledged while its handler is still running. This applies to every handler the dispatcher runs, durable or not.
- `WithFallback` is not supported for durable subscribers (`Build` returns `ErrFallbackUnsupported`): a fallback has no way to report failure.
- **Recovery.** `Build()` re-enqueues every unacknowledged delivery on the per-aggregate dispatcher, in enqueue order, before it returns. It does not wait for them to be handled, so a poison event cannot stop the process from starting; a command sent right after `Build` enqueues behind the recovered events of its aggregate in the same FIFO. Events are rebuilt from the stream (upcasters apply). An entry whose event no longer exists is acknowledged and skipped. An aggregate whose event cannot be rebuilt (a corrupt record, an upcaster error) does not stop `Build` or the other aggregates: it is parked, the error goes to `WithPublishErrorHandler`, its later entries stay pending behind the failed one, and `ResumeDeliveries` retries it (its errors are joined, and the other parked aggregates are still resumed). Acknowledgement failures go to `WithPublishErrorHandler`; the delivery stays pending and is redelivered next start.
- **At-least-once.** After a crash, a restart mid-retry, or a sibling failure, a handler can see an event again. Make handlers idempotent: dedupe on `(AggregateID, Version)` **and drop that state when you receive the deletion marker** (below).
- `SendWait` returns once every handler has been **attempted** once; it does not wait for retries, and a failing durable handler does not fail the command. If its context ends first it returns the committed event with `ErrContextCancelled`. If the aggregate is parked it returns the committed event with `ErrDeliveryParked`. Called from a handler of the same aggregate it fails at once with `ErrSendWaitFromHandler`, since it would wait on that handler itself.
- **Self-wait detection follows the context.** `ErrSendWaitFromHandler` and `ErrForgetFromHandler` are raised when the call receives the handler's context (or one derived from it). A handler that calls `SendWait` or `Forget` on its own aggregate with an unrelated `context.Background()` is not detected: it waits behind itself until its own deadline, and `Shutdown` waits on it too. The same goes for two aggregates whose handlers wait on each other. Always derive from the handler's context, and bound such calls with a timeout.
- **Handler contexts end with their delivery.** A handler's context is cancelled when its delivery ends (and when `Shutdown` gives up), so work it started on that context is released. It is not cancelled when the caller of `Send` gives up.
- **Handlers cannot break the dispatcher.** A panic in the delivery error handler or the publish error handler is swallowed; it never releases the hold on an aggregate's later events and never crashes the process.
- `Subscribe` and `Listen` remain live-only and fire-and-forget: they never see events committed while the process was down and cannot report failure. They do not influence acknowledgement.

### The deletion marker

`Forget` writes a tombstone event named `models.ForgetEventName` (`"asynx.aggregate.forget"`). **Every durable subscriber receives it, whatever its pattern**, so a subscriber that keeps per-aggregate state — a dedupe table, a cursor, a read-model row — can clear it. This matters because a re-created aggregate starts again at version 1: deduplicating on `(AggregateID, Version)` without handling the marker silently drops the new aggregate's events. Check `evt.EventName == models.ForgetEventName` first and ignore the payload otherwise; the tombstone's `Aggregate` field holds the last state. The marker is delivered once, with the same retry and parking rules, and `Forget` does not erase until every durable subscriber has accepted it.

### Reactions and re-entrancy

Enqueueing never blocks, and the aggregate lock is released before `SendWait` waits for handlers, so a handler may send commands back to its own aggregate. During a `Forget` of that aggregate such a command is rejected with `ErrForgetInProgress` (see below) rather than queued, so a handler waiting on the `Forget` can never deadlock it. A handler must not `Forget` the aggregate it is handling either: the marker queues behind that very handler, so the call fails at once with `ErrForgetFromHandler` (it may forget other aggregates). A handler must not `SendWait` to its own aggregate either: it would wait on itself behind the dispatcher queue, so that fails at once with `ErrSendWaitFromHandler`.

## Idempotent Commands

A command opts in by implementing `models.IdempotentCommand`:

```go
func (c PlaceOrder) CommandID() string { return c.RequestID }   // client-minted
```

For a non-empty id, under the aggregate lock:

1. If a receipt exists: accepted → return the original event (rebuilt from its stream) with a nil error and write nothing; rejected → return an error wrapping `ErrValidation` with the stored text, even if the command would now pass.
2. On success the accepted receipt is written in the event's transaction.
3. On `ErrValidation` a rejected receipt is written in its own transaction **before** the rejection is reported. If that write fails the command fails with `ErrPipelineFailed` instead (a rejection that could be overturned by a retry is never reported); the caller retries and re-validates.
4. Infrastructure errors, version conflicts and sync-projection failures write no receipt, so a retry runs fresh.
5. The same id used on a different aggregate is answered from the original receipt; no second write happens.

Receipts are kept as long as your store keeps them. Prune them according to your retry window. A receipt outlives `Forget`: retrying an accepted command for an erased aggregate still answers "already done" with a minimal event (`ID`, `AggregateID`, `Version`, no state) rather than running the command again and resurrecting the aggregate.

## Snapshots

Snapshots are a derived cache. They are written after the event commits, outside the transaction, and a failure never fails the command or blocks delivery. Observe failures with `WithSnapshotErrorHandler`.

## Forget

1. Under the aggregate lock: mark the aggregate as being forgotten, and commit the tombstone in one transaction that also runs the synchronous projections and **drops the aggregate's pending delivery entries and records the tombstone as a pending entry** (the deletion supersedes the older ones, durably, so no restart can deliver an older event after the marker; the marker's own entry keeps it ahead of anything that follows). Then abandon the deliveries still in memory and enqueue the tombstone, so a failing handler cannot hold it back. Release the lock.
2. Deliver the tombstone to the forget handlers and to every durable subscriber. `Forget` waits for this, **up to its context**; it fails (`ErrForgetFailed`) if the context ends or a durable subscriber exhausts its attempts on the marker.
3. Retake the lock; delete the snapshot, then run `Atomically{DeleteStream; DropDeliveries}`; clear the mark.

**The fence.** From step 1 until step 3 completes, a command for that aggregate is rejected with `ErrForgetInProgress` — it is not queued, so nothing can commit between the tombstone and the erase, and no read model keeps a row for an aggregate that no longer exists. Rejecting instead of waiting is deliberate: handlers ahead of the tombstone may react by sending commands to the aggregate while `Forget` waits for them, and a queued command would deadlock the pair. Retry after `Forget` returns; the aggregate then restarts at version 1. Other aggregates are unaffected, and `Forget` does not occupy a shard worker while it waits.

The snapshot is deleted before the stream so a half-failed erase can never leave a stale snapshot behind. If `Forget` fails after the tombstone (context ended, a durable subscriber could not take the marker, the erase itself failed, the dispatcher closed) the aggregate keeps its tombstone, **is parked** (see `PausedAggregates`) and accepts commands again: their events commit but wait behind the marker, never delivered ahead of it. `ResumeDeliveries` or a restart delivers the still-pending marker first, then those events; retrying `Forget` supersedes them all. Call `Forget` again: handlers see the marker again (at-least-once). A crash between the tombstone and the erase behaves the same, minus the park: the marker is still pending, so the restart delivers it before anything newer. The marker is never dead-lettered: `Forget` must not erase until every subscriber has really taken it.

## Migration from v0.8.x

This release changes the `Store` contract and is a compile-time break for store implementers.

- Replace `Append` and `Delete` on `Store` with `Atomically` and the `Tx` methods above. Keep your existing `events` table and the `events:` stream-ID prefix; no data migration is needed.
- Add `receipts` and `deliveries` tables (see the schema above) and implement `GetReceipt`, `PendingDeliveries`, `AckDelivery`.
- Run `storetest.Run` against the new store.
- `WithSubscriber` handlers now return an `error` (durable handlers are retried until they succeed).
- Behavior changes: snapshot write failures no longer fail `Send`; same-aggregate commands no longer fail with `ErrPipelineFailed` under concurrency within one process; commands for an aggregate being forgotten fail with `ErrForgetInProgress`; `Forget` waits for durable subscribers and honors its context; `SendWait` and `Forget` wait outside the shard workers; `Shutdown` interrupts delivery retries.
- New knobs: `WithDeliveryMaxAttempts`, `WithDeadLetterHandler`, `ResumeDeliveries`, `PausedAggregates`, and `models.ForgetEventName`.
- Durable subscribers that dedupe per aggregate must handle `models.ForgetEventName` (see above).
- `store.Memory` keeps `Append` and `Delete` as helpers for seeding test data.

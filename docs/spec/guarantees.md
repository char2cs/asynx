# Write-Path Guarantees Specification (v0.9.0)

## Overview

Asynx guarantees the following about every command, by construction rather than configuration. They hold for any `ShardingOpts` and any `WorkersPerShard`.

| # | Guarantee | Mechanism |
|---|-----------|-----------|
| 1 | **One writer per aggregate.** Commands for the same aggregate never run at the same time. | A per-aggregate lock held from load to enqueue. |
| 2 | **Ordered delivery.** Handlers see an aggregate's events in version order. | The event is enqueued to the per-aggregate dispatcher while the lock is held. |
| 3 | **One transaction.** The event, the synchronous read-model updates, the command receipt and the delivery entry commit together or not at all. | `Store.Atomically` is the only write path. |
| 4 | **Nothing durable is left undelivered.** Neither a crash after commit nor a failing durable handler loses a delivery. | Delivery log, retry with backoff, recovery in `Build()`. |
| 5 | **Idempotent commands.** A retry of a command id is a no-op, and a rejection stays rejected. | Receipts written in the event's transaction. |
| 6 | **Fenced delete.** `Forget` cannot interleave with a command on the same aggregate. | `Forget` runs under the aggregate lock. |

Scope: **one process per store.** The lock and recovery are in-process. Several processes sharing a store stay free of corruption (the `(streamID, version)` uniqueness check still arbitrates, and a loser gets `ErrPipelineFailed`) but do not get exclusivity, cross-process ordering or coordinated recovery.

## Pipeline

For each command, under the aggregate's lock:

```
(receipt check)  → Load → Validate
  → Atomically{ Append → sync projections → PutReceipt → EnqueueDelivery* }
  → best-effort snapshot Put           (failure → SnapshotErrorHandler)
  → enqueue to the per-aggregate dispatcher
  → release the lock
  → [SendWait only] wait for handlers
```

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
- `AckDelivery` and `DropDeliveries` are idempotent.
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
- **Failure holds the line.** A handler that returns an error, panics, or fails after exceeding its timeout leaves the delivery pending. The dispatcher retries just the failed handlers with exponential backoff (default 100 ms doubling to 30 s; `WithDeliveryRetry`) until they succeed. That aggregate's later events wait behind it, so version 6 is never delivered before version 5 succeeded. Other aggregates are unaffected. Observe every failed attempt with `WithDeliveryErrorHandler(func(ctx, evt, err, attempt))`; a panic also reaches `WithPanicHandler`. A permanently failing ("poison") event therefore blocks its aggregate until it is fixed or the handler is — by design, and visible through the error handler. Shutting down stops the retries and leaves the delivery pending.
- **Timeouts are alarms.** `WithHandlerTimeout` reports a slow handler (`ErrHandlerTimeout` to the delivery error handler; the timeout handler for bus subscribers) but the handler is still waited for, so the aggregate's next event never overlaps it and a delivery is never acknowledged while its handler is still running. This applies to every handler the dispatcher runs, durable or not.
- `WithFallback` is not supported for durable subscribers (`Build` returns `ErrFallbackUnsupported`): a fallback has no way to report failure.
- **Recovery.** `Build()` re-enqueues every unacknowledged delivery on the per-aggregate dispatcher, in enqueue order, before it returns. It does not wait for them to be handled, so a poison event cannot stop the process from starting; a command sent right after `Build` enqueues behind the recovered events of its aggregate in the same FIFO. Events are rebuilt from the stream (upcasters apply). An entry whose event no longer exists is acknowledged and skipped. Acknowledgement failures go to `WithPublishErrorHandler`; the delivery stays pending and is redelivered next start.
- **At-least-once.** After a crash, or when a sibling handler of the same event failed, a handler can see an event again. Make handlers idempotent (dedupe on `(AggregateID, Version)`).
- `SendWait` returns once every handler has been **attempted** once; it does not wait for retries, and a failing durable handler does not fail the command.
- `Subscribe` and `Listen` remain live-only and fire-and-forget: they never see events committed while the process was down and cannot report failure. They do not influence acknowledgement.
- Each aggregate's dispatcher queue is **unbounded**, so enqueueing never blocks under the aggregate lock. A permanently slow or failing subscriber therefore lets its aggregate's queue grow in memory, with no back-pressure on writers. Watch the delivery error handler and `PendingDeliveries` size; this is intended for moderate event volumes.

### Reactions and re-entrancy

Enqueueing never blocks (each aggregate's dispatcher queue is unbounded), and the aggregate lock is released before `SendWait` waits for handlers. A handler may therefore send commands back to its own aggregate. `Forget` writes its tombstone under the lock but releases it while the forget handlers run, so reactions queued ahead of the tombstone can still send commands to the aggregate (they are erased with it). A handler must also not `SendWait` to its own aggregate (it would wait on itself behind the dispatcher queue) — that restriction is unchanged from earlier versions.

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

`Forget` runs on the aggregate's shard worker. Under the aggregate lock it appends the tombstone (with synchronous projections) and enqueues it; the lock is released while the forget handlers observe the tombstone and the last state; it is then retaken to delete the snapshot and run `Atomically{DeleteStream; DropDeliveries}`. The erase is atomic with respect to every command, so the aggregate cannot be resurrected on top of erased history; a command that slipped in after the tombstone is erased with the rest. The snapshot is deleted before the stream so a half-failed erase can never leave a stale snapshot behind. A crash between the tombstone and the erase leaves the aggregate in place; retrying `Forget` runs the handlers again.

## Migration from v0.8.x

This release changes the `Store` contract and is a compile-time break for store implementers.

- Replace `Append` and `Delete` on `Store` with `Atomically` and the `Tx` methods above. Keep your existing `events` table and the `events:` stream-ID prefix; no data migration is needed.
- Add `receipts` and `deliveries` tables (see the schema above) and implement `GetReceipt`, `PendingDeliveries`, `AckDelivery`.
- Run `storetest.Run` against the new store.
- `WithSubscriber` handlers now return an `error` (durable handlers are retried until they succeed).
- Behavior changes: snapshot write failures no longer fail `Send`; same-aggregate commands no longer fail with `ErrPipelineFailed` under concurrency within one process; `Forget` runs on the shard worker.
- `store.Memory` keeps `Append` and `Delete` as helpers for seeding test data.

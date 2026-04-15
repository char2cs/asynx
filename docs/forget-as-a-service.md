# Forget as a Service

Forget as a Service (FaaS) lets you completely erase an aggregate and all its stored history from Asynx. It is an operational feature — intended for fixing bad data, removing test records, and handling aggregates that have reached the natural end of their lifecycle.

## What happens when you Forget

Calling `Forget` performs three steps in sequence:

1. **Tombstone write** — Asynx writes a final event named `asynx.aggregate.forget` to the event store. The event carries the aggregate's last known state as its payload.
2. **Handler notification** — All `ForgetHandler` callbacks registered for this instance are called synchronously with the tombstone event. Forget blocks until every handler completes.
3. **Erasure** — All events and snapshots for the aggregate are deleted from the backing store. After this point, `Get` returns `ErrNotFound`.

Forget is routed through the same aggregate shard as `Send` and `SendWait`, so it is naturally serialized with any in-flight commands for that aggregate — a Forget called while a command is executing will queue behind it.

## Basic usage

```go
err := ax.Forget(ctx, "order-123")
if err != nil {
    // see: Error handling
}
```

`Forget` returns `nil` once the aggregate is fully erased. The original context deadline applies to the tombstone write and handler phase; the deletion phase always runs to completion regardless of context cancellation.

## Cleaning up read models with ForgetHandler

The tombstone event is published to all registered `ForgetHandler` callbacks before any data is deleted. Use this to keep your read models consistent:

```go
ax.OnForget(func(ctx context.Context, e models.Event[Order]) {
    readModel.Delete(e.AggregateID)
    cache.Invalidate(e.AggregateID)
})
```

`e.Aggregate` holds the aggregate's **last known state** at the moment of deletion — available if your cleanup logic needs it.

`OnForget` returns a subscription ID. Pass it to `Unsubscribe` to stop receiving forget events:

```go
subID, _ := ax.OnForget(handler)

// later:
ax.Unsubscribe(subID)
```

### Registering at build time

If you know your handler at startup, use `WithForgetHandler` on the builder instead:

```go
ax, err := asynx.New[Order]().
    WithEventStore(store).
    WithForgetHandler(func(ctx context.Context, e models.Event[Order]) {
        readModel.Delete(e.AggregateID)
    }).
    Build()
```

## Error handling

| Error | Meaning |
|---|---|
| `ErrValidation` | The aggregate does not exist — nothing was written or deleted |
| `ErrShuttingDown` | `Forget` was called after `Shutdown` |
| `ErrContextCancelled` | The context was cancelled before the command reached the shard |
| `ErrQueueFull` | The aggregate's shard queue was full — retry |
| `ErrPipelineFailed` | The tombstone write to the store failed — nothing was deleted |
| `ErrForgetFailed` | The tombstone was written and handlers ran, but the store deletion failed |

`ErrForgetFailed` is the most important error to handle. When it occurs, the tombstone event exists in the event store but the aggregate's data has not been erased. A subsequent `Forget` call will return `ErrValidation` (the aggregate appears gone to the command pipeline). Recovery requires deleting the residual data directly via your store implementation.

## What Forget does NOT do

- **It does not affect other aggregates.** Only the specified aggregate ID is erased.
- **It does not stop in-flight commands.** Commands already enqueued ahead of Forget will complete first.
- **It does not clean up your read models automatically.** Register a `ForgetHandler` to do that.
- **It is not a soft delete.** There is no recovery path once erasure completes.

## The `asynx.*` event namespace

The tombstone event name `asynx.aggregate.forget` is in a namespace reserved for internal Asynx events. Do not `Subscribe` to it directly — use `OnForget` or `WithForgetHandler` instead. This ensures your handler receives the correctly typed callback and remains decoupled from internal naming.

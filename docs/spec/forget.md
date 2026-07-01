# Forget as a Service Specification

## Overview

Forget as a Service (FaaS) adds the ability to completely erase an aggregate and all its stored state from Asynx. It is an operational feature — intended for fixing bad data, removing test records, and handling aggregate lifecycle ends.

The operation performs three steps in sequence:

1. Writes a tombstone event (`asynx.aggregate.forget`) carrying the aggregate's last known state
2. Publishes the tombstone synchronously, waiting for all `ForgetHandler` callbacks to complete
3. Deletes all events, snapshots, and cached state for the aggregate from the store

Forget is routed through the same shard as any other command for that aggregate, so it is naturally serialized against in-flight commands — no command races ahead of a Forget that has been dispatched.

The `asynx.*` event name namespace is reserved for internal Asynx processes. Callers never subscribe to `"asynx.aggregate.forget"` directly — they register via `OnForget` or `WithForgetHandler`.

---

## Public API

### `Asynx[T]` Interface — New Methods

#### `Forget`

```go
Forget(ctx context.Context, aggregateID string) error
```

**Purpose**
Erases all stored state for the given aggregate. Notifies all registered `ForgetHandler` callbacks before deletion so they can clean up downstream read models.

**Parameters**
- `ctx` — context for cancellation and timeouts; passed through shard routing and tombstone publication
- `aggregateID` — the ID of the aggregate to erase; must be non-empty

**Return Values**
- `error` — nil on success; see Error Handling for specific sentinels

**Invariants**
- **Aggregate must exist** — returns `ErrValidation` if the aggregate has no recorded events
- **Shard-serialized** — Forget is routed through the same aggregate shard as `Send`/`SendWait`; it queues behind any in-flight commands for that aggregate
- **Handlers complete before deletion** — all `ForgetHandler` callbacks run to completion before store deletion begins
- **Deletion is total** — all events, snapshots, and in-memory cache for the aggregate are removed; `Get` returns not-found after Forget returns nil

**Side Effects**
- Writes a tombstone event to the event store
- Publishes the tombstone to the bus; all subscriptions matching `"asynx.aggregate.forget"` are called (including `ForgetHandler` registrations)
- Deletes all events and snapshots for the aggregate from both the event store and snapshot store
- Evicts the aggregate from the in-memory cache

**Error Handling**

| Error | When |
|---|---|
| `ErrValidation` | Aggregate does not exist |
| `ErrShuttingDown` | `Forget` called during shutdown |
| `ErrContextCancelled` | Context cancelled before shard picks up the command |
| `ErrQueueFull` | Shard queue full at dispatch time |
| `ErrPipelineFailed` | Tombstone write to store failed |
| `ErrForgetFailed` | Tombstone published and handlers ran, but store deletion failed |

`ErrForgetFailed` signals a partially-erased state: the tombstone has been published and all `ForgetHandler` callbacks have completed, but the store deletion failed. Callers should treat this as a retriable store error.

**Example**
```go
err := ax.Forget(ctx, "order-123")
if errors.Is(err, models.ErrValidation) {
    // aggregate never existed
}
if errors.Is(err, models.ErrForgetFailed) {
    // tombstone was published; handlers ran; but store deletion failed — retry
}
```

---

#### `OnForget`

```go
OnForget(fn models.ForgetHandler[T]) (string, error)
```

**Purpose**
Registers a callback to be invoked whenever an aggregate is forgotten. Returns a subscription ID that can be passed to `Unsubscribe` to deregister the handler. Multiple handlers may be registered.

**Parameters**
- `fn` — non-nil `ForgetHandler[T]` callback

**Return Values**
- `string` — subscription ID, unique for the lifetime of this registration
- `error` — non-nil if registration failed (e.g., bus is closed)

**Invariants**
- **Handler is called synchronously** — `Forget` blocks until all `ForgetHandler` callbacks complete before proceeding to store deletion
- **Handler receives last known state** — `Event.Aggregate` holds the aggregate state at the moment of deletion
- **Unsubscribable** — the returned ID can be passed to `Unsubscribe` to stop receiving forget events

**Example**
```go
id, err := ax.OnForget(func(ctx context.Context, e models.Event[Order]) {
    readModel.Delete(e.AggregateID)
})
// later, to stop listening:
ax.Unsubscribe(id)
```

---

### `Builder[T]` — New Method

#### `WithForgetHandler`

```go
func (b *Builder[T]) WithForgetHandler(fn models.ForgetHandler[T]) *Builder[T]
```

**Purpose**
Registers a `ForgetHandler` at build time. Convenience sugar for the common single-handler case — equivalent to calling `OnForget` after `Build`. Multiple calls to `WithForgetHandler` register multiple handlers.

**Parameters**
- `fn` — non-nil `ForgetHandler[T]` callback

**Example**
```go
ax, err := asynx.New[Order]().
    WithEventStore(store).
    WithForgetHandler(func(ctx context.Context, e models.Event[Order]) {
        readModel.Delete(e.AggregateID)
    }).
    Build()
```

---

## `models.ForgetHandler[T]`

New named type in `models/handlers.go`, following the existing `*Handler[T]` convention:

```go
// ForgetHandler is called when an aggregate is forgotten.
// It receives the tombstone event; Event.Aggregate holds the aggregate's last known state.
type ForgetHandler[T any] func(context.Context, Event[T])
```

Same signature as `ProjectionHandler[T]`. The tombstone event provides the final aggregate state so callers have what they need to clean up read models or trigger downstream side effects.

---

## `models.Store` Interface — New Method

```go
// Delete removes all records for the given aggregateID from the store.
// Implementations must be idempotent — deleting a non-existent aggregate is not an error.
Delete(ctx context.Context, aggregateID string) error
```

Called by `EventStore.Delete` on both the event store and snapshot store (which may be the same backing instance). Implementations must handle the no-op case cleanly.

---

## Internal Design

### `forgetCommand[T]`

A package-private struct in the root package implementing `models.Command[T]`. It is not exported — callers use `Forget(ctx, aggregateID)` and never instantiate this type directly.

It stores the last aggregate state across `Validate` → `EmitEvent` so the tombstone payload carries the final state:

```go
type forgetCommand[T any] struct {
    aggregateID string
    last        *T
}

func (c *forgetCommand[T]) AggregateID() string  { return c.aggregateID }
func (c *forgetCommand[T]) EventName() string     { return "asynx.aggregate.forget" }
func (c *forgetCommand[T]) ShouldSnapshot() bool  { return false }

func (c *forgetCommand[T]) Validate(current *T) error {
    if current == nil {
        return fmt.Errorf("%w: aggregate %s not found", models.ErrValidation, c.aggregateID)
    }
    c.last = current
    return nil
}

func (c *forgetCommand[T]) EmitEvent(current *T) T { return *c.last }
```

### Execution Flow

```
Forget(ctx, aggregateID)
  │
  ├─ proc.SendWait(ctx, &forgetCommand{aggregateID})
  │     │
  │     ├─ shard.Route(aggregateID)            ← same shard as Send/SendWait
  │     ├─ es.Write(ctx, cmd)                  ← tombstone event written
  │     └─ bus.PublishSync(ctx, tombstone)     ← ForgetHandlers called synchronously
  │
  └─ es.Delete(ctx, aggregateID)
        ├─ eventStore.Delete(ctx, aggregateID)
        ├─ snapshotStore.Delete(ctx, aggregateID)
        └─ evict from in-memory cache
```

### Event Namespace

`asynx.*` is reserved for events emitted internally by Asynx. Currently only `"asynx.aggregate.forget"` exists. This namespace must not be used by application-level commands.

---

## Error Sentinels

`ErrForgetFailed` is a new sentinel in `models/errors.go`:

```go
var ErrForgetFailed = errors.New("forget failed")
```

It is returned when the tombstone has been published and all handlers have completed, but the subsequent store deletion failed. It wraps the underlying store error:

```go
return fmt.Errorf("%w: %w", models.ErrForgetFailed, err)
```

---

## Testing

### Unit Tests (`asynx_test.go`)

- `Forget` on a non-existent aggregate returns `ErrValidation`
- `Forget` on an existing aggregate: `ForgetHandler` is called with the correct last state; subsequent `Get` returns not-found
- `Forget` during shutdown returns `ErrShuttingDown`
- `OnForget` registers a handler; `Unsubscribe` with returned ID stops it from receiving future forget events
- `WithForgetHandler` on Builder is equivalent to calling `OnForget` after `Build`
- Concurrent `Forget` calls on the same aggregate: second returns `ErrValidation`

### Concurrency Tests

- `Send` followed immediately by `Forget` on the same aggregate: shard serialization guarantees `Send` completes before the tombstone is written

### Store Tests (`store/memory_test.go`)

- `Delete` removes all events and snapshots for the aggregate
- `Delete` on a non-existent aggregate is a no-op (returns nil)

### `WaitPublish` Compatibility

`Forget` uses `SendWait`, which publishes synchronously — `WaitPublish` in tests already covers it with no additional changes.

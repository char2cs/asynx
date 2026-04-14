# Forget as a Service — Design Spec

**Date:** 2026-04-14
**Branch:** feature/faas
**Status:** Approved

## Overview

Forget as a Service (FaaS) adds the ability to completely erase an aggregate and all its stored state from Asynx. It is an operational feature — intended for fixing bad data, removing test records, and handling aggregate lifecycle ends. It is not a compliance/GDPR feature.

The operation:
1. Writes a tombstone event (`asynx.aggregate.forget`) carrying the aggregate's last known state
2. Publishes the tombstone synchronously, waiting for all `ForgetHandler` callbacks to complete
3. Deletes all events, snapshots, and cached state for the aggregate

The `asynx.*` event name namespace is reserved for internal Asynx processes. Callers never subscribe to `"asynx.aggregate.forget"` directly — they register via `OnForget` or `WithForgetHandler`.

---

## Public API

### `Asynx[T]` interface

Two new methods:

```go
// Forget writes a tombstone event for the aggregate, notifies all ForgetHandlers
// synchronously, then erases all events, snapshots, and cached state.
// Returns ErrValidation if the aggregate does not exist.
Forget(ctx context.Context, aggregateID string) error

// OnForget registers a handler invoked when any aggregate is forgotten.
// Returns a subscription ID that can be passed to Unsubscribe.
OnForget(fn models.ForgetHandler[T]) (string, error)
```

### `Builder[T]`

One new method, as build-time sugar for the common single-handler case:

```go
// WithForgetHandler registers a ForgetHandler at build time.
// Equivalent to calling OnForget after Build.
func (b *Builder[T]) WithForgetHandler(fn models.ForgetHandler[T]) *Builder[T]
```

### `models.ForgetHandler[T]`

New named type in `models/handlers.go`, following the existing `*Handler[T]` pattern:

```go
// ForgetHandler is called when an aggregate is forgotten.
// It receives the tombstone event, whose Data field holds the aggregate's last known state.
type ForgetHandler[T any] func(context.Context, Event[T])
```

Same signature as `ProjectionHandler[T]`. Receives the tombstone event so callers have the final state to clean up read models.

---

## Internal Execution Flow

### `forgetCommand[T]`

A package-private struct in the root package implementing `models.Command[T]`. It stores the last aggregate state across `Validate` → `EmitEvent` so the tombstone payload carries the final state.

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

### `asynxImpl.Forget`

```go
func (i *asynxImpl[T]) Forget(ctx context.Context, aggregateID string) error {
    _, err := i.proc.SendWait(ctx, &forgetCommand[T]{aggregateID: aggregateID})
    if err != nil {
        return err
    }
    if err := i.es.Delete(ctx, aggregateID); err != nil {
        return fmt.Errorf("%w: %w", models.ErrForgetFailed, err)
    }
    return nil
}
```

`SendWait` handles shard routing (serialized with all other commands for that aggregate), tombstone write, and synchronous projection/forget-handler notification. Only after all handlers complete does `es.Delete` run.

### `EventStore.Delete`

New method on `eventstore.EventStore[T]`:

1. Calls `store.Delete(ctx, aggregateID)` on the event store
2. Calls `store.Delete(ctx, aggregateID)` on the snapshot store (may be the same backing store — implementations must be idempotent)
3. Evicts the aggregate from the in-memory cache

### `models.Store` interface

One new method:

```go
// Delete removes all records for the given aggregateID.
// Implementations must be idempotent — deleting a non-existent aggregate is not an error.
Delete(ctx context.Context, aggregateID string) error
```

---

## Event Namespace

`asynx.*` is reserved for internal Asynx events. Currently only `"asynx.aggregate.forget"` exists. Users must not manually `Subscribe` to this pattern — use `OnForget` instead.

---

## Error Handling

| Error | When |
|---|---|
| `ErrValidation` | Aggregate does not exist |
| `ErrShuttingDown` | `Forget` called during shutdown |
| `ErrContextCancelled` | Context cancelled before shard picks up the command |
| `ErrQueueFull` | Shard queue full at dispatch time |
| `ErrPipelineFailed` | Tombstone write to store failed |
| `ErrForgetFailed` (new) | Tombstone published, handlers ran, but `es.Delete` failed |

`ErrForgetFailed` is a new sentinel in `models/errors.go`. It is distinct from `ErrPipelineFailed`: when it occurs, the tombstone has already been published and all `ForgetHandler` callbacks have completed. The aggregate is in a partially-erased state. Callers should treat this as a retriable store error.

---

## Testing Plan

### Unit tests (`asynx_test.go`)

- `Forget` on a non-existent aggregate returns `ErrValidation`
- `Forget` on an existing aggregate: tombstone event published, `ForgetHandler` called with correct last state, subsequent `Get` returns not-found
- `Forget` during shutdown returns `ErrShuttingDown`
- `OnForget` registers a handler; `Unsubscribe` with returned ID stops it from receiving future forget events
- `WithForgetHandler` on Builder is equivalent to `OnForget` after Build

### Concurrency tests

- `Send` followed immediately by `Forget` on the same aggregate: shard serialization guarantees `Send` completes before the tombstone is written
- Concurrent `Forget` calls on the same aggregate: second returns `ErrValidation` (aggregate already erased)

### Store tests (`internal/store/memory_test.go`)

- `Delete` removes all events and snapshots for the aggregate
- `Delete` on a non-existent aggregate is a no-op (returns nil)

### `WaitPublish` compatibility

`Forget` uses `SendWait`, which is synchronous — `WaitPublish` in tests already covers it with no additional changes needed.

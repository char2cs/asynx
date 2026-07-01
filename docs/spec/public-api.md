# Public API Specification

## Overview

The `Asynx[T]` interface is the single unified surface developers interact with. It aggregates methods from multiple internal modules (`processor`, `eventstore`, `bus`) into one coherent API. Developers import only the top-level `asynx` package (plus `models` for shared types) and work with `Asynx[T]`.

**Design Principle:** Developers should not know or care about internal module boundaries. The public API hides the complexity of which module implements which method.

---

## The `Asynx[T]` Interface

```go
type Asynx[T any] interface {
    // Commands
    Send(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)
    SendWait(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)

    // Erasure
    Forget(ctx context.Context, aggregateID string) error
    OnForget(fn models.ForgetHandler[T]) (string, error)

    // Queries
    Get(ctx context.Context, aggregateID string) (T, error)
    Exists(ctx context.Context, aggregateID string) (bool, error)
    Preload(ctx context.Context, aggregateID string) error

    // Subscriptions
    Subscribe(pattern string, handler models.ProjectionHandler[T],
        opts ...models.SubscriptionOpt[T]) (string, error)
    Unsubscribe(id string) error
    Listen(pattern string, count int) (<-chan models.Event[T], func(), error)
    SubscribeWait(ctx context.Context, pattern string) (models.Event[T], error)

    // Replay
    Replay(ctx context.Context, aggregateID string, fromVersion, toVersion int64,
        fn models.ProjectionHandler[T]) error

    // Lifecycle
    Shutdown(ctx context.Context) error
    WaitPublish() // testing only
}
```

**Created via the builder:**

```go
ax, err := asynx.New[Order]().
    WithEventStore(store).
    WithBus(bus).
    Build()  // Returns Asynx[T]
```

---

## Public API Methods

### Command Execution

#### `Send(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)`

**Defined in:** `processor` module
**Purpose:** Issue a command to the system. Returns the resulting event once it is durably written. Projection handlers fire asynchronously — when `Send` returns, handlers may not have run yet.

```go
event, err := ax.Send(ctx, createOrderCmd)
if errors.Is(err, models.ErrValidation) {
    // Command invalid
} else if errors.Is(err, models.ErrPipelineFailed) {
    // Write conflict, retry from scratch
}
```

**Returns:** `ErrValidation`, `ErrPipelineFailed`, `ErrQueueFull`, `ErrShuttingDown`, `ErrContextCancelled`, or `nil`

---

#### `SendWait(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)`

**Defined in:** `processor` module
**Purpose:** Same as `Send`, but additionally blocks until all matching projection handlers have completed. When `SendWait` returns without error, the event is persisted and every projection subscribed to it has finished.

```go
event, err := ax.SendWait(ctx, createOrderCmd)
// Read models are now consistent with this event
```

---

#### `Shutdown(ctx context.Context) error`

**Defined in:** `processor` module
**Purpose:** Gracefully shut down the instance

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err := ax.Shutdown(ctx)
```

---

### Aggregate Erasure

#### `Forget(ctx context.Context, aggregateID string) error`

**Defined in:** `processor` + `eventstore` modules
**Purpose:** Write a tombstone event, notify all `ForgetHandler`s synchronously, then erase all events, snapshots, and cached state for the aggregate. See [forget.md](./forget.md).

```go
err := ax.Forget(ctx, "order_123")
if errors.Is(err, models.ErrValidation) {
    // Aggregate does not exist
}
if errors.Is(err, models.ErrForgetFailed) {
    // Tombstone written, deletion failed — retry Forget
}
```

---

#### `OnForget(fn models.ForgetHandler[T]) (string, error)`

**Defined in:** `bus` module
**Purpose:** Register a handler invoked when any aggregate is forgotten. The handler receives the tombstone event; `Event.Aggregate` holds the last known state. Returns a subscription ID usable with `Unsubscribe`.

---

### State Queries (Strong Consistency)

#### `Get(ctx context.Context, aggregateID string) (T, error)`

**Defined in:** `eventstore.reader` (reader sub-module)
**Purpose:** Load the current aggregate state

```go
order, err := ax.Get(ctx, "order_123")
if errors.Is(err, models.ErrNotFound) {
    // Aggregate doesn't exist
}
```

**Returns:** Current state of the aggregate, or `ErrNotFound` if never existed

---

#### `Exists(ctx context.Context, aggregateID string) (bool, error)`

**Defined in:** `eventstore.reader` (reader sub-module)
**Purpose:** Check if an aggregate exists without loading full state

```go
exists, err := ax.Exists(ctx, "order_123")
if !exists {
    return errors.New("order not found")
}
```

---

#### `Preload(ctx context.Context, aggregateID string) error`

**Defined in:** `eventstore.reader` (reader sub-module)
**Purpose:** Eagerly rehydrate aggregate state (pay cold path cost offline)

```go
// At startup, preload hot aggregates
err := ax.Preload(ctx, "hot_order_123")
// Subsequent Send() calls will use warm path (snapshot + delta)
```

---

### Event Subscription (Eventual Consistency)

#### `Subscribe(pattern string, handler models.ProjectionHandler[T], opts ...models.SubscriptionOpt[T]) (string, error)`

**Defined in:** `bus` module
**Purpose:** Register a callback for events matching a regex pattern. For dotted, topic-style event names, wrap the pattern in `asynx.Topic()`.

```go
id, err := ax.Subscribe("OrderPlaced", func(ctx context.Context, e models.Event[Order]) {
    // Handle event
    updateReadModel(e)
})

defer ax.Unsubscribe(id)
```

**Options:**
- `models.WithFallback(handler)` — Fallback if primary fails
- `models.WithHandlerTimeout(duration)` — Timeout for handler execution

---

#### `Unsubscribe(id string) error`

**Defined in:** `bus` module
**Purpose:** Remove a subscription by ID

```go
err := ax.Unsubscribe(subscriptionID)
```

---

#### `Listen(pattern string, count int) (<-chan models.Event[T], func(), error)`

**Defined in:** `asynx` (over the `bus` module)
**Purpose:** Channel-based subscription. The pattern is converted via `Topic()` internally.

- `count > 0`: channel capacity equals `count`; auto-closes and auto-unsubscribes after `count` events.
- `count <= 0`: unbounded — capacity 16, never auto-closes. Call the returned unsubscribe func to clean up; do not range over the channel after calling it.

```go
ch, unsub, err := ax.Listen("order.created.*", 3)
defer unsub()
for e := range ch {
    fmt.Println(e.EventName)
}
```

**Returns:** `ErrEmptyPattern` when the pattern is empty. The unsubscribe func is idempotent.

---

#### `SubscribeWait(ctx context.Context, pattern string) (models.Event[T], error)`

**Defined in:** `asynx` (over `Listen`)
**Purpose:** Block until the first event matching the pattern arrives or the context is done. Auto-unsubscribes in all cases. Bound the wait with a context deadline.

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
event, err := ax.SubscribeWait(ctx, "order.shipped.*")
```

---

### Event Replay (Read-Only Recovery)

#### `Replay(ctx context.Context, aggregateID string, fromVersion int64, toVersion int64, fn models.ProjectionHandler[T]) error`

**Defined in:** `eventstore.replayer` (replayer sub-module)
**Purpose:** Iterate events for manual re-projection

```go
// Replay to recover projection after failure
err := ax.Replay(ctx, "order_123", 0, 0, func(ctx context.Context, e models.Event[Order]) {
    // Re-run projection logic
    readModel.UpdateOrder(e)
})
```

**Parameters:**
- `fromVersion` — Inclusive start version (0 = first event)
- `toVersion` — Inclusive end version (0 = latest event)

---

### Test Synchronization

#### `WaitPublish()`

**Defined in:** `processor` + `bus` modules
**Purpose:** Block until all async event publishes and handlers complete. Only for use in tests; do not call in production code.

---

## Module Boundaries (Hidden from Developer)

| Public Method | Internal Module | Sub-Module |
|---|---|---|
| `Send()` / `SendWait()` | `processor` | — |
| `Forget()` | `processor` + `eventstore` | — |
| `Shutdown()` | `processor` | — |
| `Get()` | `eventstore` | `reader` |
| `Exists()` | `eventstore` | `reader` |
| `Preload()` | `eventstore` | `reader` |
| `Subscribe()` / `Unsubscribe()` / `OnForget()` | `bus` | — |
| `Listen()` / `SubscribeWait()` | `asynx` over `bus` | — |
| `Replay()` | `eventstore` | `replayer` |
| `WaitPublish()` | `processor` + `bus` | — |

**Developer view:** None of this is visible. The API is unified on `Asynx[T]`.

---

## Example: Complete Usage

```go
// Build instance
ax, err := asynx.New[Order]().
    WithEventStore(postgresStore).
    Build()
if err != nil {
    log.Fatal(err)
}

// Subscribe to events (projection)
ax.Subscribe("OrderPlaced", func(ctx context.Context, e models.Event[Order]) {
    updateReadModel(e.Aggregate)
})

// Load state (strong consistency read)
order, err := ax.Get(ctx, orderID)
if err != nil {
    log.Fatal(err)
}

// Issue command (validation → write → async publish)
_, err = ax.Send(ctx, updateOrderCmd)
if errors.Is(err, models.ErrValidation) {
    // Validation failed, try different command
    _, err = ax.Send(ctx, differentCmd)
}
if errors.Is(err, models.ErrPipelineFailed) {
    // Write conflict with a concurrent writer — safe to retry
    _, err = ax.Send(ctx, updateOrderCmd)
}

// Recover projection after failure (read-only replay)
err = ax.Replay(ctx, orderID, 0, 0, func(ctx context.Context, e models.Event[Order]) {
    updateReadModel(e.Aggregate)
})

// Graceful shutdown
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err = ax.Shutdown(ctx)
```

---

## Error Types

All errors are sentinel values in the `models` package; match with `errors.Is`. See [models/errors.go](../../models/errors.go) for the authoritative list.

**Where the common ones come from:**
- `ErrValidation` — from `cmd.Validate()` failure, or `Forget` on a missing aggregate
- `ErrPipelineFailed` — from a store `Append` failure (e.g. version conflict)
- `ErrQueueFull` — from processor shard queue hitting capacity
- `ErrShuttingDown` — from processor during shutdown phase 1
- `ErrAlreadyShuttingDown` — from a second concurrent `Shutdown` call
- `ErrContextCancelled` — from context cancellation
- `ErrNotFound` — from eventstore when aggregate has no events
- `ErrForgetFailed` — from `Forget` when deletion fails after the tombstone
- `ErrEmptyPattern` / `ErrNilHandler` — from subscription calls with invalid input
- `ErrBusClosed` / `ErrDispatcherClosed` — from operations after shutdown
- `ErrMissingEventStore` — from `Build()` without `WithEventStore()`

---

## Design Philosophy

**One API, Multiple Internal Modules:**

The processor, eventstore, and bus modules exist for **implementation clarity** and **internal organization**. They are not exposed to developers because:

1. **Developers think in terms of: Send a command, query state, subscribe to events**
2. **Asynx handles: Which module implements which operation**
3. **Result: Simple, unified API without exposing internal complexity**

**If the implementation changes (e.g., eventstore internals refactored), the public API remains stable.** Developers never break because internal modules were reorganized.

---

## Thread Safety

All public methods on `Asynx[T]` are thread-safe and can be called concurrently:

```go
go func() { ax.Send(ctx1, cmd1) }()
go func() { ax.Send(ctx2, cmd2) }()
go func() { ax.Get(ctx3, agg1) }()
go func() { ax.Subscribe(pattern, handler) }()
// All safe
```

---

## Lifecycle

```go
// 1. Create via builder
ax, _ := asynx.New[Order]().WithEventStore(store).Build()

// 2. Use for command execution and queries
ax.Send(ctx, cmd)
ax.Get(ctx, id)
ax.Subscribe(pattern, handler)

// 3. Graceful shutdown
ax.Shutdown(ctx)

// 4. No further operations after Shutdown
ax.Send(ctx, cmd)  // Returns ErrShuttingDown
```

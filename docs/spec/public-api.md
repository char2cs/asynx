# Public API Specification

## Overview

The `Instance[T]` type is the single unified interface developers interact with. It aggregates methods from multiple internal modules (`processor`, `eventstore`, `projection`) into one coherent API. Developers import only the top-level `asynx` package and work with `Instance[T]`.

**Design Principle:** Developers should not know or care about internal module boundaries. The public API hides the complexity of which module implements which method.

---

## The `Instance[T]` Type

```go
type Instance[T any] struct {
    // All public methods are defined on this type
    // Internal modules (processor, eventstore, projection, etc.) are private fields
}
```

**Created via the builder:**

```go
instance, err := asynx.New[Order]().
    WithEventStore(store).
    WithBus(bus).
    Build()  // Returns Instance[T]
```

---

## Public API Methods

### Command Execution

#### `Send(ctx context.Context, cmd Command[T]) error`

**Defined in:** `processor` module
**Purpose:** Issue a command to the system

```go
err := instance.Send(ctx, createOrderCmd)
if err == asynx.ErrValidation {
    // Command invalid
} else if err == asynx.ErrPipelineFailed {
    // Write conflict, retry from scratch
}
```

**Returns:** `ErrValidation`, `ErrPipelineFailed`, `ErrQueueFull`, `ErrShuttingDown`, `ErrContextCancelled`, or `nil`

---

#### `Shutdown(ctx context.Context) error`

**Defined in:** `processor` module
**Purpose:** Gracefully shut down the instance

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err := instance.Shutdown(ctx)
```

---

### State Queries (Strong Consistency)

#### `Get(ctx context.Context, aggregateID string) (T, error)`

**Defined in:** `eventstore.reader` (reader sub-module)
**Purpose:** Load the current aggregate state

```go
order, err := instance.Get(ctx, "order_123")
if err == asynx.ErrNotFound {
    // Aggregate doesn't exist
}
```

**Returns:** Current state of the aggregate, or `ErrNotFound` if never existed

---

#### `Exists(ctx context.Context, aggregateID string) (bool, error)`

**Defined in:** `eventstore.reader` (reader sub-module)
**Purpose:** Check if an aggregate exists without loading full state

```go
exists, err := instance.Exists(ctx, "order_123")
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
err := instance.Preload(ctx, "hot_order_123")
// Subsequent Send() calls will use warm path (snapshot + delta)
```

---

### Event Subscription (Eventual Consistency)

#### `Subscribe(pattern string, primaryHandler func(Event[T]), opts ...SubscriptionOpt) (string, error)`

**Defined in:** `projection` module
**Purpose:** Register a callback for events matching a pattern

```go
id, err := instance.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Handle event
    updateReadModel(e)
})

defer instance.Unsubscribe(id)
```

**Options:**
- `WithFallback(handler func(Event[T]))` — Fallback if primary fails
- `WithHandlerTimeout(duration)` — Timeout for handler execution

---

#### `Unsubscribe(id string) error`

**Defined in:** `projection` module
**Purpose:** Remove a subscription by ID

```go
err := instance.Unsubscribe(subscriptionID)
```

---

### Event Replay (Read-Only Recovery)

#### `Replay(ctx context.Context, aggregateID string, fromVersion int64, toVersion int64, fn func(Event[T])) error`

**Defined in:** `eventstore.replayer` (replayer sub-module)
**Purpose:** Iterate events for manual re-projection

```go
// Replay to recover projection after failure
err := instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    // Re-run projection logic
    readModel.UpdateOrder(e)
})
```

**Parameters:**
- `fromVersion` — Inclusive start version (0 = first event)
- `toVersion` — Inclusive end version (0 = latest event)

---

## Module Boundaries (Hidden from Developer)

| Public Method | Internal Module | Sub-Module |
|---|---|---|
| `Send()` | `processor` | — |
| `Shutdown()` | `processor` | — |
| `Get()` | `eventstore` | `reader` |
| `Exists()` | `eventstore` | `reader` |
| `Preload()` | `eventstore` | `reader` |
| `Subscribe()` | `projection` | — |
| `Unsubscribe()` | `projection` | — |
| `Replay()` | `eventstore` | `replayer` |

**Developer view:** None of this is visible. The API is unified on `Instance[T]`.

---

## Example: Complete Usage

```go
// Build instance
instance, err := asynx.New[Order]().
    WithEventStore(postgresStore).
    WithBus(kafkaBus).
    Build()
if err != nil {
    log.Fatal(err)
}

// Subscribe to events (projection)
instance.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    updateReadModel(e.Aggregate)
})

// Load state (strong consistency read)
order, err := instance.Get(ctx, orderID)
if err != nil {
    log.Fatal(err)
}

// Issue command (validation → write → async publish)
err = instance.Send(ctx, updateOrderCmd)
if err == asynx.ErrValidation {
    // Validation failed, try different command
    err = instance.Send(ctx, differentCmd)
}
if err == asynx.ErrPipelineFailed {
    // Write failed, retry from scratch (automatic on retry)
    err = instance.Send(ctx, updateOrderCmd)
}

// Recover projection after failure (read-only replay)
err = instance.Replay(ctx, orderID, 0, 0, func(e asynx.Event[Order]) {
    updateReadModel(e.Aggregate)
})

// Graceful shutdown
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err = instance.Shutdown(ctx)
```

---

## Error Types

All errors returned from public API methods:

```go
var ErrValidation = errors.New("asynx: command validation failed")
var ErrPipelineFailed = errors.New("asynx: event write failed, retry from scratch")
var ErrQueueFull = errors.New("asynx: shard queue full, back off and retry")
var ErrShuttingDown = errors.New("asynx: instance shutting down, no new commands accepted")
var ErrContextCancelled = errors.New("asynx: context cancelled, command removed from queue")
var ErrNotFound = errors.New("asynx: aggregate not found")
```

**Where they come from:**
- `ErrValidation` — from `cmd.Validate()` failure
- `ErrPipelineFailed` — from `eventstore.Write()` failure
- `ErrQueueFull` — from processor shard queue hitting capacity
- `ErrShuttingDown` — from processor during shutdown phase 1
- `ErrContextCancelled` — from context cancellation
- `ErrNotFound` — from eventstore when aggregate has no events

---

## Design Philosophy

**One API, Multiple Internal Modules:**

The processor, eventstore, projection, and bus modules exist for **implementation clarity** and **internal organization**. They are not exposed to developers because:

1. **Developers think in terms of: Send a command, query state, subscribe to events**
2. **Asynx handles: Which module implements which operation**
3. **Result: Simple, unified API without exposing internal complexity**

**If the implementation changes (e.g., eventstore internals refactored), the public API remains stable.** Developers never break because internal modules were reorganized.

---

## Thread Safety

All public methods on `Instance[T]` are thread-safe and can be called concurrently:

```go
go func() { instance.Send(ctx1, cmd1) }()
go func() { instance.Send(ctx2, cmd2) }()
go func() { instance.Get(ctx3, agg1) }()
go func() { instance.Subscribe(pattern, handler) }()
// All safe
```

---

## Lifecycle

```go
// 1. Create via builder
instance, _ := asynx.New[Order]().WithEventStore(store).Build()

// 2. Use for command execution and queries
instance.Send(ctx, cmd)
instance.Get(ctx, id)
instance.Subscribe(pattern, handler)

// 3. Graceful shutdown
instance.Shutdown(ctx)

// 4. No further operations after Shutdown
instance.Send(ctx, cmd)  // Returns ErrShuttingDown
```

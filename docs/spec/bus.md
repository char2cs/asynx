# Bus Package Specification

## Overview

The `bus` package provides the default in-process event dispatcher for Asynx. It's a channel-based, synchronous pub/sub implementation that delivers events to subscribed callbacks within the same process.

The Bus interface (`Bus[T]`) is defined in `core` — the bus package provides the concrete in-process implementation. Developers can replace it with an external broker (Kafka, NATS, Redis Streams) for multi-node deployments by implementing the `Bus[T]` interface and passing it to `WithBus()` on the builder.

The default bus is suitable for single-node applications and tests. For multi-node deployments, developers implement or use an external bus that provides durable, cross-node event delivery.

## Default In-Process Bus Implementation

### Creation

```go
// Create a new in-process bus for aggregate type Order
bus := asynx.NewChannelBus[Order]()

// Use in builder:
instance, err := asynx.New[Order]().
    WithEventStore(store).
    WithBus(bus).  // explicit
    Build()

// Or omit WithBus() to use default:
instance, err := asynx.New[Order]().
    WithEventStore(store).
    Build()  // uses NewChannelBus[Order]() by default
```

### Guarantees

The default in-process bus provides:

1. **Synchronous dispatch** — Publish blocks until all handlers return
2. **Panic recovery** — Handler panics don't crash the bus; they're recovered and passed to panic handler
3. **Pattern matching** — Supports exact event names and regex patterns
4. **No durability** — Events published to the bus that crash mid-callback are lost (use Replay to recover)
5. **No ordering across aggregates** — Events from different aggregates may dispatch out of order
6. **In-process only** — Events are not visible to other processes (no multi-node delivery)

### Limitations

The default bus is NOT suitable for:
- Multi-node deployments (events don't cross process boundaries)
- Projections that require durability (callback failures are not automatically replayed)
- High-throughput scenarios where blocking on handler execution causes bottlenecks

For these use cases, implement a custom Bus or use an external broker.

---

## Subscription ID Management

### Default In-Process Bus

In the default in-process bus, subscription IDs are:

- **Unique within the process** — valid as long as the subscription exists
- **Not durable across restarts** — IDs are meaningless after process restart
- **Format is implementation detail** — could be UUID, counter, or any string

Example:

```go
id1, _ := bus.Subscribe("OrderPlaced", handler1)
id2, _ := bus.Subscribe("OrderCancelled", handler2)

// IDs are unique within this process:
// id1 = "sub_1" (or "550e8400-e29b...", or some other format)
// id2 = "sub_2"

// After restart, those IDs are invalid — new subscriptions get new IDs
```

### External Bus Implementations

External buses (Kafka, NATS, Redis) can provide durable subscription IDs:

```go
// Kafka bus example:
bus := kafkabus.New[Order]("kafka-broker:9092")
id, _ := bus.Subscribe("OrderPlaced", handler)
// id = "consumer-group-orders-1"  // persistent group name
// Survives process restart because consumer group is server-side
```

Developers choosing an external bus should verify its subscription ID durability guarantees match their requirements.

---

## Pattern Matching

### Exact Match

Subscribe to a single event name:

```go
bus.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Fires only on OrderPlaced events
    log.Printf("Order %s placed\n", e.AggregateID)
})
```

Pattern string is the exact event name from `Command.EventName()`.

### Regex Match

Subscribe to multiple related events with a regex pattern:

```go
bus.Subscribe("^Order(Placed|Cancelled|Shipped)$", func(e asynx.Event[Order]) {
    // Fires on OrderPlaced, OrderCancelled, OrderShipped
    switch e.EventName {
    case "OrderPlaced":
        handlePlaced(e)
    case "OrderCancelled":
        handleCancelled(e)
    case "OrderShipped":
        handleShipped(e)
    }
})

bus.Subscribe("^.*", func(e asynx.Event[Order]) {
    // Fires on ALL events (greedy pattern)
    auditLog.Record(e)
})
```

**Regex evaluation:**
- Full-string match (anchored at both ends)
- Tested against the event's `EventName` field
- Compiled once at subscribe time (not per-event)
- Invalid regex returns error from Subscribe

### Performance Consideration

Regex matching is more expensive than exact matching. For high-throughput scenarios, exact matches are preferred. Wildcard patterns should be used sparingly.

---

## Handler Execution Model

### Synchronous Dispatch

Handlers are called synchronously during `Publish`. The processor blocks waiting for `Publish` to return before the caller gets the success response.

```
Processor.Send(cmd)
  ↓
Validate & emit event
  ↓
Write event to eventstore (save point)
  ↓
Return nil to caller ← caller unblocks here
  ↓
Bus.Publish(event) → calls matching handlers synchronously
  ↓
Handler returns
  ↓
Next handler (if any)
  ↓
Publish returns
  ↓
Processor's async work completes
```

**Impact:** If a handler blocks, the processor cannot return to the caller until it returns. This is correct — the event is already safely written to the eventstore, so the caller is unblocked before dispatch. But long-running handlers can tie up processor goroutines.

### No Handler Ordering Guarantee

Multiple handlers matching the same event may execute in any order. Do not rely on a specific order. If handler A must run before handler B, combine them into a single handler.

### Panic Recovery

If a handler panics, Asynx recovers it internally:

```go
bus.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // If this panics:
    panic("something went wrong")

    // Asynx recovers it, and:
    // 1. Calls the configured panic handler (if provided)
    // 2. Continues to the next matching handler
    // 3. Does NOT propagate to caller
})

// Elsewhere:
instance, _ := asynx.New[Order]().
    WithEventStore(store).
    WithPanicHandler(func(e asynx.PanicEvent[Order]) {
        log.Printf("panic on %s: %v\n", e.EventName, e.Err)
    }).
    Build()
```

Handler panics don't crash the system, but they do indicate bugs that need fixing.

---

## Fallback Handler Dispatch

The projection system (not the bus itself) manages fallback handlers. When a primary handler panics or times out, the bus publishes the same event to a fallback handler if one is registered. See the `projection` specification for fallback handler details.

---

## Thread Safety

The default in-process bus is thread-safe:

- Multiple goroutines can call Subscribe, Unsubscribe, and Publish concurrently
- Internal state is protected by locks (sync.RWMutex or similar)
- No deadlock risk from concurrent bus operations and handler execution

However, handlers themselves are responsible for their own thread safety. If a handler accesses shared state, the handler must protect that state.

---

## Subscription and Unsubscription

### Subscribe Idempotency

Calling `Subscribe` with the same pattern and handler multiple times creates multiple subscriptions:

```go
id1, _ := bus.Subscribe("OrderPlaced", handler)
id2, _ := bus.Subscribe("OrderPlaced", handler)  // Different ID, separate subscription

// Publish fires the handler twice (once per subscription)
```

### Unsubscribe Idempotency

Calling `Unsubscribe` with an invalid ID:

```go
bus.Unsubscribe("non_existent_id")
// Returns error (or nil, implementation-dependent)
```

Calling Unsubscribe multiple times with the same ID:

```go
id, _ := bus.Subscribe("OrderPlaced", handler)

bus.Unsubscribe(id)       // OK
bus.Unsubscribe(id)       // Error or nil (impl-dependent)
```

### Unsubscribe During Dispatch

If a handler unsubscribes itself during execution:

```go
var id string
id, _ = bus.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Fire once, then unsubscribe
    bus.Unsubscribe(id)
})
```

The unsubscribe takes effect immediately — this subscription will not be called for future events, but it's already executing for the current event.

### Unsubscribe Does Not Interrupt In-Flight Handlers

If a handler is currently running and another goroutine calls `Unsubscribe`, the running handler finishes. The handler cannot be interrupted mid-execution.

---

## Shutdown

### Close Semantics

Calling `Close` on the bus:

1. Stops accepting new subscriptions and publications
2. Waits for in-flight handlers to finish (respects context deadline)
3. Releases all resources

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := bus.Close(ctx)
if err != nil {
    // Context deadline exceeded, or handlers hung
    log.Printf("bus shutdown failed: %v\n", err)
}

// After Close, all methods return error
bus.Subscribe("OrderPlaced", handler)  // Error
bus.Publish(ctx, event)                // Error
bus.Unsubscribe(id)                    // Error
bus.Close(ctx)                         // Error
```

### Handler Timeout During Shutdown

If a handler exceeds the shutdown deadline, the implementation may force-kill it (details are implementation-dependent). The in-process bus waits for handlers to return voluntarily up to the deadline.

---

## No Message Durability

The default in-process bus does not durably store events:

```
Publish fires handlers
  ↓
Handler crashes before returning
  ↓
Event is LOST (not replayed automatically)
```

Recovery requires:
1. Detect that the handler failed (application logs, monitoring)
2. Call `asynx.Replay(ctx, aggregateID, fromVersion, toVersion, handler)` to manually re-run the projection

This is by design — Asynx guarantees the event is in the eventstore, but projection callbacks are the developer's responsibility.

External buses (Kafka, NATS) provide durability via consumer groups or similar — they track which events each consumer has processed and allow replay. Developers using external buses get better durability out of the box.

---

## Multi-Node Behavior

### Default In-Process Bus

Events are published only to subscribers within the same process:

```
Node A: Processor publishes OrderPlaced
  ↓
Node A: Subscribers on Node A receive it
  ↓
Node B: Subscribers on Node B do NOT receive it
```

Each node has its own bus instance. Events don't cross process boundaries.

### External Bus Implementations

Swap the default bus for an external broker to fan events across nodes:

```go
instance, _ := asynx.New[Order]().
    WithEventStore(postgresStore).
    WithBus(kafkaBus).  // Events published to Kafka
    Build()

// Node A publishes OrderPlaced to Kafka
// Node B subscribes to Kafka and receives it
// Node C subscribes to Kafka and receives it
// All nodes have eventually-consistent read models
```

---

## Example: Complete Setup

```go
// Create instance with default in-process bus:
instance, err := asynx.New[Order]().
    WithEventStore(postgresStore).
    WithSchemaVersion(1).
    WithPanicHandler(func(e asynx.PanicEvent[Order]) {
        log.Printf("projection panic on %s: %v\n", e.EventName, e.Err)
        metrics.IncrementPanicCount()
    }).
    Build()

// Subscribe to events (handlers are called via the default bus):
instance.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Update read model
    readModel.RecordOrder(e.Aggregate)
    log.Printf("Order %s placed\n", e.AggregateID)
})

instance.Subscribe("^Order.*", func(e asynx.Event[Order]) {
    // Audit log all order events
    auditLog.Log(e.EventName, e.AggregateID)
})

// Send commands (triggers projection callbacks synchronously):
err := instance.Send(ctx, createOrderCmd)
if err != nil {
    log.Printf("send failed: %v\n", err)
}

// Handlers have already been called synchronously
// Read models are updated
// If a handler panicked, panic handler was notified

// Later, graceful shutdown:
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
instance.Shutdown(ctx)
```

---

## Implementation Notes for Custom Bus

If implementing a custom Bus[T] (e.g., Kafka-backed):

1. **Thread safety** — all methods must be safe for concurrent calls
2. **Panic isolation** — panics in one handler must not affect others
3. **Pattern matching** — support both exact and regex patterns
4. **Subscription IDs** — return unique IDs per subscription
5. **No event replay on subscribe** — subscriptions only receive future events (or events from resume point, for durable buses)
6. **Context awareness** — respect context cancellation in Publish and Close
7. **Error semantics** — return errors for invalid patterns, storage failures, etc.

Example custom implementation (Redis Streams):

```go
type RedisStreamsBus[T any] struct {
    client      *redis.Client
    consumerID  string
    subs        map[string]*subscription[T]
    mu          sync.RWMutex
}

func (b *RedisStreamsBus[T]) Publish(ctx context.Context, event Event[T]) error {
    data, _ := json.Marshal(event)
    return b.client.XAdd(ctx, &redis.XAddArgs{
        Stream: "events:" + event.AggregateID,
        Values: map[string]interface{}{"data": string(data)},
    }).Err()
}

func (b *RedisStreamsBus[T]) Subscribe(pattern string, handler func(Event[T])) (string, error) {
    id := generateID()
    b.mu.Lock()
    b.subs[id] = &subscription[T]{pattern, handler}
    b.mu.Unlock()

    // Start consuming from Redis
    go b.consume(id)
    return id, nil
}

// ... etc
```

---

## Known Limitations

**No guaranteed delivery.** The default in-process bus offers no durability guarantees. Events published to handlers that crash are lost. Developers must implement recovery via `asynx.Replay()`.

**No causality ordering.** The bus does not order events by causality — only by version per aggregate. Cross-aggregate causal ordering is not provided.

**Synchronous dispatch blocks.**  In the default implementation, handlers block the processor. Long-running handlers tie up worker goroutines. For asynchronous dispatch, use an external bus with async consumer groups (Kafka) or pipe events to a separate worker pool.

# Processor Package Specification

## Overview

The `processor` package owns the sharded worker pool, shard routing, and the `Send()` method — the main entry point for developers to issue commands. It's responsible for:

- Routing commands to shards based on aggregate ID
- Guaranteeing serial ordering per aggregate (no concurrent writes)
- Synchronously validating and emitting events
- Durably writing events to the eventstore
- Asynchronously dispatching to bus and projections
- Version generation and assignment
- Graceful shutdown with ordered draining

The processor depends on `core`, `eventstore`, and `bus`.

---

## Command Execution Pipeline

### Overview

The processor implements a two-phase pipeline:

1. **Synchronous Phase** — caller blocks until event is written to eventstore
2. **Asynchronous Phase** — caller is already free; bus dispatch and projection callbacks run

### Synchronous Phase (Caller Blocks Here)

```
1. Call eventstore.Get(ctx, cmd.AggregateID()) → load current state
   If error (storage, upcaster panic, or auto-snapshot write failure):
     return ErrPipelineFailed to caller

2. Call cmd.Validate(currentState) → validate command
   If error: return ErrValidation to caller

3. Call cmd.EmitEvent(currentState) → generate new state

4. Call eventstore.Write(...) → commit to eventstore
   If error: return ErrPipelineFailed to caller

5. Return nil to caller ← caller unblocks here, event is durable
```

**Step 1 (Get) can return error when:**
- Storage unavailable
- Upcaster panics (schema migration error)
- Auto-snapshot write fails (state is correct; snapshot is optimization)

All Get() errors are treated as **retriable** — caller should retry from scratch with fresh context.

This phase blocks the caller. If any step takes a long time (cold path rehydration, storage latency), the caller waits.

### Asynchronous Phase (Caller Is Already Free)

```
6. Call bus.Publish(event) → dispatch to projection subscriptions (async)
   Handlers spawned in background goroutines, Publish() returns immediately
7. Projection handler callbacks execute asynchronously
```

Projection handlers run concurrently in the bus's handler goroutines. If a handler panics or times out, it's logged and other handlers are unaffected. Bus.Publish() errors are not visible to the Send() caller (event is already durable).

**Context Propagation:**
- The processor calls `bus.Publish(context.WithoutCancel(ctx), event)`
- This preserves caller's trace values (request ID, span ID, etc.) for observability
- But detaches from caller's deadline/cancellation (handlers run independently)
- If caller cancels, Send() returns immediately; async publish continues in background

### Durability Guarantee

`nil` return from `Send()` means: **the event is in the eventstore, safe, and will survive a crash.** The caller never needs to wonder.

---

## Public API

```go
// Send issues a command and waits for it to be durably written
Send(ctx context.Context, cmd Command[T]) error
// Returns: ErrValidation, ErrPipelineFailed, ErrQueueFull, ErrShuttingDown, ErrContextCancelled, or nil

// Shutdown gracefully drains all pending commands and projections
Shutdown(ctx context.Context) error
```

---

## Method: `Send`

**Signature**
```go
Send(ctx context.Context, cmd Command[T]) error
```

**Purpose**
Issues a command to the processor. Blocks until the event is durably written to the eventstore. Returns success only if the event is safe.

**Parameters**
- `ctx` — context for cancellation and timeouts
  - If cancelled before write completes, command is removed from queue and `ErrContextCancelled` is returned
  - If deadline is exceeded, context error is returned
- `cmd` — Command[T] implementation containing:
  - `AggregateID()` → which aggregate this targets
  - `Validate(currentState *T)` → validation logic
  - `EmitEvent(currentState *T) T` → state transition
  - `EventName() string` → name for this event
  - `ShouldSnapshot() bool` → snapshot hint

**Return Values**
- `nil` — success, event is durable
- `error` — one of the error types below

**Error Types**

1. **`ErrValidation`** — Command validation failed
   - Returned from `cmd.Validate(currentState)`
   - Event was not created
   - Caller can retry with a different command

2. **`ErrPipelineFailed`** — Event write to eventstore failed
   - Typically: version conflict (another node raced to same version)
   - Could also be: storage unavailable, context cancelled mid-write
   - **Recovery: Caller MUST retry by calling Send() from scratch**
     - Reload state via eventstore.Get()
     - Revalidate the command
     - Re-emit the event
     - Call Send() again (with same command struct or fresh one)
     - Do NOT retry by bumping version number (that corrupts the stream)

3. **`ErrQueueFull`** — Shard queue depth exceeded
   - Returned if `WithShardingOpts(QueueDepth=N)` is set and queue is full
   - Command was rejected before processing
   - Signals: system is overloaded
   - **Recovery: Caller should NOT retry blindly**
     - Back off, shed load, scale up, then retry
     - Retrying immediately likely fails again

4. **`ErrShuttingDown`** — Instance is shutting down
   - Returned if Shutdown() was called and intake is closed
   - Command was not queued
   - No recovery possible (instance is going down)

5. **`ErrAlreadyShuttingDown`** — Shutdown already initiated
   - **Explicit guarantee:** Shutdown() can only be called once
   - Returned if Shutdown() is called a second time (while first is still in progress or already done)
   - Only the first Shutdown() call proceeds with full shutdown sequence
   - Subsequent calls return this error immediately without executing shutdown logic
   - Safe to check/ignore — first caller owns the shutdown, others can discard the error

6. **`ErrContextCancelled`** — Caller's context was cancelled
   - Command was removed from queue before processing
   - Event was not written
   - **Recovery: Caller can retry with a fresh context**

### Execution Guarantees

**Version Atomicity**
```
Version numbers are generated and written atomically
No two different aggregates ever have the same version
No two different (aggregateID, version) pairs ever exist
```

This is enforced by:
- Processor atomically increments version before write
- Store enforces (aggregateID, version) uniqueness
- Failed writes are detected and surface as error

**Serial Ordering Per Aggregate**
```
Commands for the same aggregate are routed to the same shard
Shard queues are FIFO, processed by a single worker
No concurrent writes to the same aggregate
Version order is guaranteed per aggregate
```

This is guaranteed by:
- Hash(AggregateID) → determines shard (deterministic)
- Same aggregate always hashes to same shard
- Shard queue is processed sequentially

**Ordering Not Guaranteed Across Aggregates**
```
Command A targets aggregate_1
Command B targets aggregate_2
Both issued in order: A, B
Might execute in order: B, A (different shards)
No cross-aggregate ordering guarantee
```

This is intentional — it allows parallelism for different aggregates.

### Cold Path Warning

If an aggregate has never been accessed before and has a long event history:

```
Send(ctx, cmd)
  ↓
eventstore.Get(aggregateID)
  ↓
No snapshot exists (cold path)
  ↓
Replayer must iterate all 10000 events
  ↓
Caller blocks (potentially seconds)
```

**Mitigation:**
- Call `eventstore.Preload(ctx, aggregateID)` at startup for hot aggregates
- Use `ShouldSnapshot()` on commands to create checkpoints after first cold access
- Verify aggregates don't have pathologically long histories (design issue)

**Example:**
```go
// If you have "hot" aggregates that are updated frequently,
// preload them at startup to pay cold path cost offline:
err := instance.Preload(ctx, "user_123")
err := instance.Preload(ctx, "cart_456")

// Subsequent Send() calls will use warm path (snapshot + delta)
```

### Context Handling

`Send()` respects the caller's context:

```go
// With timeout:
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := instance.Send(ctx, cmd)
// If validation/write takes > 5s, returns context deadline error

// With cancellation:
ctx, cancel := context.WithCancel(context.Background())
go func() {
    time.Sleep(1 * time.Second)
    cancel()  // User clicked cancel button
}()
err := instance.Send(ctx, cmd)
// If cancelled while queued, returns ErrContextCancelled

// With cancellation mid-write:
// Context cancelled while eventstore.Write is in progress
// Write may or may not have completed (depends on store)
// If not written: context error returned, caller can retry
// If already written: event is durable (save point already passed)
```

### Example: Basic Usage

```go
// Create order command:
cmd := CreateOrderCommand{
    OrderID:  "order_123",
    Items:    []Item{{SKU: "widget", Qty: 2}},
    Total:    99.99,
}

// Send command:
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := instance.Send(ctx, cmd)

switch err {
case nil:
    // Success, order is created and durable
    log.Println("Order created")

case asynx.ErrValidation:
    // Validation failed (e.g., invalid SKU)
    log.Println("Validation failed")

case asynx.ErrPipelineFailed:
    // Write failed, likely version conflict (multi-node race)
    // MUST retry from scratch:
    log.Println("Write failed, retrying...")
    err = instance.Send(ctx, cmd)  // Reload state, revalidate, try again

case asynx.ErrQueueFull:
    // System overloaded
    log.Println("Queue full, backing off...")
    time.Sleep(time.Second)
    err = instance.Send(ctx, cmd)  // Retry after backoff

case asynx.ErrShuttingDown:
    // Instance is shutting down
    log.Println("Cannot send, shutting down")

case context.Canceled, context.DeadlineExceeded:
    // Caller cancelled or timed out
    log.Println("Context error:", err)
}
```

---

## Method: `Shutdown`

**Signature**
```go
Shutdown(ctx context.Context) error
```

**Purpose**
Gracefully shuts down the processor. Stops accepting new commands, drains all pending work, then closes.

**Parameters**
- `ctx` — context with timeout for shutdown
  - Determines how long to wait for draining
  - If deadline exceeded before draining completes, error is returned

**Return Values**
- `error` — non-nil if shutdown times out or fails

### Shutdown Sequence

**Phase 1 — Stop Intake** (immediate)
```
1. Mark processor as shutting down
2. New Send() calls return ErrShuttingDown immediately
3. In-flight Send() calls continue
```

**Phase 2 — Drain Shards**
```
1. Wait for all shard queues to empty
2. Wait for all in-flight commands to finish processing (reach save point)
3. Each shard worker completes its current command and stops
```

**Phase 3 — Drain Bus**
```go
1. Wait for all in-flight events to finish dispatching
2. All projection callbacks must return (or recover from panic)
3. Bus closes
```

All three phases must complete before Shutdown returns `nil`.

### Shutdown Guarantees

- **No new commands accepted** — Send() returns ErrShuttingDown immediately
- **All queued commands processed** — shard queues are drained
- **All in-flight projections finish** — bus drain waits for callbacks
- **Graceful, not forced** — handlers are allowed to finish, no forceful kill

### Shutdown Timeout

If shutdown context deadline is exceeded:

```
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := instance.Shutdown(ctx)
if err != nil {
    // Shutdown timed out
    // Some handlers may still be running (not forcefully killed)
    // Operator decides: retry, log, force kill, etc.
}
```

### Example: Graceful Server Shutdown

```go
// In a web server, hook Shutdown on graceful shutdown signal:
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

// Server running...

// SIGTERM received:
<-sigChan

// Stop accepting new requests:
server.Stop()

// Drain Asynx (30 second timeout):
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := instance.Shutdown(ctx); err != nil {
    log.Printf("shutdown timeout: %v\n", err)
    // Some work may not have completed
    // Operator: force kill if necessary
    os.Exit(1)
}

log.Println("Shutdown complete")
os.Exit(0)
```

---

## Sharding Model

### Purpose

Sharding provides parallelism without allocating a goroutine per aggregate. The processor:

1. **Routes commands by aggregate ID** — Hash(AggregateID) → shard number
2. **Queues in shards** — commands for same aggregate queue together
3. **Workers process sequentially** — each shard has one worker, processes queue FIFO
4. **Guarantees serial ordering** — all commands for same aggregate run sequentially (no concurrent writes)

### Configuration

```go
asynx.ShardingOpts{
    Shards:     8,       // Number of shard queues (default 8)
    QueueDepth: 0,       // Max items per shard (0 = unbounded, default 0)
}
```

**Shards:** Typically set to number of CPU cores or higher for throughput. Each shard is independent.

**QueueDepth:** Limits memory usage. If set:
- Full queue → Send() returns `ErrQueueFull` immediately
- Backpressure → caller must back off
- Unbounded (0) → queue grows until memory exhausted (monitor memory usage)

### Example Configuration

```go
asynx.New[Order]().
    WithEventStore(store).
    WithShardingOpts(asynx.ShardingOpts{
        Shards:     16,   // 16 workers for parallelism
        QueueDepth: 1000, // Max 1000 commands per shard before backpressure
    }).
    Build()
```

### Sharding Invariant

All commands targeting the same aggregate hash to the same shard. Commands for different aggregates may hash to different shards and execute in parallel.

```
Aggregate 1: Order_123 → Shard 0 (serial)
Aggregate 2: Order_456 → Shard 3 (serial)
Aggregate 3: Order_789 → Shard 0 (queued after Order_123)

Result: Order_123 and Order_789 execute serially (same shard)
        Order_456 executes in parallel (different shard)
```

---

## Error Recovery Patterns

### Pattern 1: ErrValidation

```go
err := instance.Send(ctx, cmd)
if err == asynx.ErrValidation {
    // Command is invalid given current state
    // Example: trying to ship an order that's already shipped
    log.Println("Invalid command:", err)
    // Inform user, return error, don't retry
    return
}
```

### Pattern 2: ErrPipelineFailed

```go
// WRONG: Don't do this
for i := 0; i < 3; i++ {
    err := instance.Send(ctx, cmd)
    if err == nil {
        break
    }
    if err == asynx.ErrPipelineFailed {
        // WRONG: Just retrying with the same cmd
        // cmd was validated against stale state, might be invalid now
        continue
    }
}

// RIGHT: Retry from scratch
for i := 0; i < 3; i++ {
    err := instance.Send(ctx, cmd)
    if err == nil {
        break
    }
    if err == asynx.ErrPipelineFailed {
        // CORRECT: cmd will be revalidated against fresh state
        // same cmd struct, but state has changed
        continue
    }
}
// Note: This is still questionable in real scenarios
// Better to have caller handle ErrPipelineFailed explicitly

// BEST: Handle with specific logic
err := instance.Send(ctx, cmd)
if err == asynx.ErrPipelineFailed {
    // Command validation and write both ran, but write failed
    // Likely cause: version conflict in multi-node deployment
    // Solution: exponential backoff before retrying
    backoff := time.Millisecond * 100 * time.Duration(math.Pow(2, float64(attemptCount)))
    time.Sleep(backoff)
    // Retry (cmd will be revalidated and re-emitted)
    return instance.Send(ctx, cmd)
}
```

### Pattern 3: ErrQueueFull

```go
err := instance.Send(ctx, cmd)
if err == asynx.ErrQueueFull {
    // System is overloaded
    // Don't retry immediately, you'll get the same error
    log.Println("Queue full, system overloaded")

    // Options:
    // 1. Back off and retry
    time.Sleep(time.Millisecond * 100)
    return instance.Send(ctx, cmd)

    // 2. Return 503 Service Unavailable to client
    http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)

    // 3. Queue command externally for later
    persistentQueue.Enqueue(cmd)
}
```

### Pattern 4: ErrShuttingDown

```go
err := instance.Send(ctx, cmd)
if err == asynx.ErrShuttingDown {
    // Instance is shutting down, no recovery
    // Could queue externally for next startup
    log.Println("Instance shutting down, queueing for later")
    persistentQueue.Enqueue(cmd)
    return
}
```

### Pattern 5: ErrContextCancelled

```go
err := instance.Send(ctx, cmd)
if err == asynx.ErrContextCancelled {
    // Caller cancelled (e.g., user hit cancel button)
    // Can retry with fresh context
    ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    return instance.Send(ctx2, cmd)
}
```

---

## Multi-Node Behavior

### Version Conflicts in Multi-Node Deployments

When two nodes issue commands for the same aggregate simultaneously:

```
Node A: Send(OrderShipCmd for order_123)
Node B: Send(OrderShipCmd for order_123)

Both call eventstore.Get(order_123) → both see version 5
Both validate and emit event → both produce version 6
Node A calls Store.Append(order_123, 6, ...) → succeeds
Node B calls Store.Append(order_123, 6, ...) → UNIQUENESS VIOLATION

Node B gets ErrPipelineFailed
Developer retries: instance.Send(ctx, cmd)
  ↓
Reloads state → now sees Node A's event at version 6
Revalidates → command still valid or invalid, depending on state
Re-emits → produces version 7 (next version)
Tries again → succeeds
```

The (aggregateID, version) uniqueness constraint is the only coordination needed. No distributed locking, no consensus protocol — the store's atomicity is sufficient.

---

## Testing with Processor

### In-Memory Store for Tests

```go
instance, _ := asynx.New[Order]().
    WithEventStore(asynx.NewMemoryStore()).
    Build()

// Now test Send(), Replay(), etc.
err := instance.Send(ctx, cmd)
// No external infrastructure needed
```

### Mocking Commands

```go
type MockCommand struct {
    aggregateID    string
    validateResult error
    emitResult     Order
    eventName      string
}

func (m MockCommand) AggregateID() string { return m.aggregateID }
func (m MockCommand) Validate(current *Order) error { return m.validateResult }
func (m MockCommand) EmitEvent(current *Order) Order { return m.emitResult }
func (m MockCommand) EventName() string { return m.eventName }
func (m MockCommand) ShouldSnapshot() bool { return false }

// Test validation failure:
cmd := MockCommand{
    aggregateID:    "order_123",
    validateResult: asynx.ErrValidation,
}
err := instance.Send(ctx, cmd)
if err != asynx.ErrValidation {
    t.Fatal("expected validation error")
}
```

---

## Known Limitations

**No built-in retry logic.** Developers must implement retry strategies at the Send() call site for ErrPipelineFailed, ErrQueueFull, and other transient errors.

**No ordering across aggregates.** Sharding provides parallelism by design. Commands for different aggregates may execute out of order. If you need cross-aggregate causal ordering, design larger aggregates or implement ordering at the application layer.

**Cold path blocks caller.** If an aggregate has a long history and no snapshot, first access replays all events synchronously. For high-frequency aggregates, use ShouldSnapshot() and Preload().

**Queued commands are lost on crash.** Commands accepted into shard queues but not yet processed are in-memory only. An unexpected crash loses them. For crash durability, persist commands externally before calling Send().

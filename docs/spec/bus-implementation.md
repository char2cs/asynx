# Bus Implementation Specification

## Overview

This specification details the **internal architecture and mechanics** of the bus module. It explains how the default in-process channel-based bus works, how subscriptions are stored and matched, how handlers are executed, and how graceful shutdown is coordinated.

The bus is the event dispatcher that connects the processor (which publishes events) to projections (which subscribe to events). The bus interface is pluggable, but this spec documents the default `ChannelBus[T]` implementation suitable for single-node applications.

**Key Design Principles:**
- **Lazy pattern compilation** — patterns compiled on first use, cached
- **Recover & continue** — handler panic doesn't block other handlers
- **RWLock synchronization** — concurrent publishes allowed, safe subscription changes
- **Wait for in-flight** — graceful shutdown waits for all handlers to complete
- **Concurrent handler execution** — handlers run in parallel via goroutines

---

## Architecture Diagram

```
┌───────────────────────────────────────────────────────┐
│                  ChannelBus[T]                        │
│  (subscriptions, handlers, pattern cache, shutdown)   │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
    ┌───▼──────┐ ┌──▼────┐ ┌────▼──────┐
    │Subscribe │ │Publish│ │ Unsubscribe
    │          │ │       │ │
    │ • RLock  │ │ RLock │ │ • Lock
    │ • Add    │ │ • Get │ │ • Remove
    │ • Cache  │ │ • Exec│ │
    └──────────┘ └──┬────┘ └───────────┘
                    │
        ┌───────────┼──────────┐
        │           │          │
    ┌───▼──┐  ┌────▼───┐  ┌──▼────┐
    │Handler│ │Handler │ │Handler │
    │   1   │ │   2    │ │  ...   │
    │(async)│ │(async) │ │(async) │
    └───────┘ └────────┘ └────────┘
        │         │         │
        └─────────┼─────────┘
                  │
            ┌─────▼─────┐
            │ WaitGroup │
            │(in-flight)│
            └───────────┘
```

---

## Core Types

### ChannelBus[T] Structure

```go
type ChannelBus[T any] struct {
    // Synchronization
    mu          sync.RWMutex                              // Protects subscriptions

    // Subscriptions
    subscriptions map[string]*subscription[T]            // subscriptionID → handler
    nextSubID     int64                                   // Counter for subscription IDs

    // Pattern Compilation Cache
    compiledPatterns map[string]*regexp.Regexp          // Lazy-compiled regex patterns

    // In-Flight Handler Tracking
    inFlightWg  *sync.WaitGroup                          // Tracks active handler goroutines
    closed      bool                                      // Bus closed (no new publishes)
}

type subscription[T any] struct {
    pattern        string                                 // Exact match or regex pattern
    handler        func(Event[T])                         // Callback function
    fallbackHandler func(Event[T])                        // Optional fallback if primary fails
    timeout        time.Duration                          // Handler execution timeout (0 = no timeout)
}
```

**Invariants:**
- All subscription operations are guarded by `mu`
- Publish uses RLock (allows concurrent publishes)
- Subscribe/Unsubscribe use Lock (exclusive)
- Pattern compilation is cached (lazy)
- Each handler execution tracked in `inFlightWg`

---

## Sub-Module: Subscription Management

### Subscribe Method

```go
func (b *ChannelBus[T]) Subscribe(
    pattern string,
    handler func(Event[T]),
    opts ...SubscriptionOpt,
) (string, error) {
    if pattern == "" {
        return "", ErrEmptyPattern
    }

    if handler == nil {
        return "", ErrNilHandler
    }

    // Parse options
    var fallback func(Event[T])
    var timeout time.Duration
    for _, opt := range opts {
        opt(&subscription[T]{})  // Apply options (simplified)
    }

    // Acquire exclusive lock (no concurrent operations)
    b.mu.Lock()
    defer b.mu.Unlock()

    // Check if closed
    if b.closed {
        return "", ErrBusClosed
    }

    // Generate subscription ID
    b.nextSubID++
    subID := fmt.Sprintf("sub_%d", b.nextSubID)

    // Create subscription
    sub := &subscription[T]{
        pattern:         pattern,
        handler:         handler,
        fallbackHandler: fallback,
        timeout:         timeout,
    }

    // Store subscription
    b.subscriptions[subID] = sub

    return subID, nil
}
```

**Behavior:**
- Pattern can be exact match (e.g., "OrderPlaced") or regex (e.g., "^Order.*")
- Handler must not be nil
- Subscription ID is unique (counter-based, guaranteed unique within process)
- Fallback handler optional (runs if primary panics)
- Timeout optional (0 = no timeout)

### Unsubscribe Method

```go
func (b *ChannelBus[T]) Unsubscribe(id string) error {
    b.mu.Lock()
    defer b.mu.Unlock()

    // Remove subscription (idempotent, no error if not found)
    delete(b.subscriptions, id)

    return nil
}
```

**Behavior:**
- Always succeeds (idempotent)
- No error if subscription ID doesn't exist
- Safe to call during publish (copy-on-read handles concurrent access)

---

## Sub-Module: Pattern Matching

### Pattern Types

```go
// Exact match: "OrderPlaced"
// Regex pattern: "^Order.*", "^(Order|Payment).*"

func (b *ChannelBus[T]) matchesPattern(eventName string, pattern string) bool {
    // Check exact match first (fast path)
    if eventName == pattern {
        return true
    }

    // Try regex match (slow path)
    // Use lazy compilation: compile on first use, cache result
    compiled, err := b.getCompiledPattern(pattern)
    if err != nil {
        // Invalid regex, treat as literal match failed
        return false
    }

    return compiled.MatchString(eventName)
}

func (b *ChannelBus[T]) getCompiledPattern(pattern string) (*regexp.Regexp, error) {
    // Check cache first
    if compiled, ok := b.compiledPatterns[pattern]; ok {
        return compiled, nil
    }

    // Not cached, compile now (lazy compilation)
    compiled, err := regexp.Compile(pattern)
    if err != nil {
        return nil, err
    }

    // Cache for future use
    b.compiledPatterns[pattern] = compiled

    return compiled, nil
}
```

**Strategy: Lazy Compilation (Option C)**
- Patterns stored as strings at subscription time
- Compiled on first publish (if pattern doesn't match exact event name)
- Compiled regex cached in `compiledPatterns` map
- Fast path: exact match (O(1) string comparison)
- Slow path: regex match (O(n) in pattern size, but only once per pattern)

**Benefits:**
- Low memory overhead if many patterns registered but few publishes to them
- Unbounded latency on first publish with new pattern (one-time cost)
- Subsequent publishes with same pattern use cached regex (fast)

---

## Sub-Module: Publishing

### Publish Method

```go
func (b *ChannelBus[T]) Publish(ctx context.Context, event Event[T]) error {
    // Use read lock (allows concurrent publishes)
    b.mu.RLock()

    // Check if closed
    if b.closed {
        b.mu.RUnlock()
        return ErrBusClosed
    }

    // Copy subscriptions (snapshot for iteration)
    // This prevents deadlock if Unsubscribe called during iteration
    var matchingHandlers []*handlerJob[T]

    for _, sub := range b.subscriptions {
        // Match pattern against event name
        if b.matchesPattern(event.EventName, sub.pattern) {
            matchingHandlers = append(matchingHandlers, &handlerJob[T]{
                handler:         sub.handler,
                fallbackHandler: sub.fallbackHandler,
                timeout:         sub.timeout,
                event:           event,
            })
        }
    }

    // Release lock before executing handlers
    b.mu.RUnlock()

    // No handlers matched
    if len(matchingHandlers) == 0 {
        return nil
    }

    // Execute handlers concurrently (Option B)
    for _, job := range matchingHandlers {
        b.inFlightWg.Add(1)
        go b.executeHandler(job)
    }

    // Publish returns immediately (async handlers)
    return nil
}

type handlerJob[T any] struct {
    handler         func(Event[T])
    fallbackHandler func(Event[T])
    timeout         time.Duration
    event           Event[T]
}

func (b *ChannelBus[T]) executeHandler(job *handlerJob[T]) {
    defer b.inFlightWg.Done()

    // Recover from panic (Option A: recover & continue)
    defer func() {
        if r := recover(); r != nil {
            // Handler panicked
            b.onHandlerPanic(r, job)

            // Try fallback if available
            if job.fallbackHandler != nil {
                b.executeWithTimeout(job.fallbackHandler, job.event, job.timeout)
            }
        }
    }()

    // Execute handler with optional timeout
    b.executeWithTimeout(job.handler, job.event, job.timeout)
}

func (b *ChannelBus[T]) executeWithTimeout(
    handler func(Event[T]),
    event Event[T],
    timeout time.Duration,
) {
    if timeout == 0 {
        // No timeout, execute directly
        handler(event)
        return
    }

    // Execute with timeout
    done := make(chan struct{})

    go func() {
        defer func() {
            if r := recover(); r != nil {
                // Panic in handler, propagate through channel
                close(done)
            }
        }()

        handler(event)
        close(done)
    }()

    select {
    case <-done:
        // Handler completed
        return

    case <-time.After(timeout):
        // Timeout exceeded
        b.onHandlerTimeout(handler, event, timeout)
        // Continue anyway (don't block publish)
    }
}

func (b *ChannelBus[T]) onHandlerPanic(panicValue interface{}, job *handlerJob[T]) {
    // Log panic
    log.Printf("projection handler panicked: %v\n", panicValue)

    // Record metric
    metrics.IncrementProjectionHandlerPanicCount(job.event.EventName)

    // Continue to next handler (don't propagate)
}

func (b *ChannelBus[T]) onHandlerTimeout(handler func(Event[T]), event Event[T], timeout time.Duration) {
    // Log timeout
    log.Printf("projection handler timed out after %v\n", timeout)

    // Record metric
    metrics.IncrementProjectionHandlerTimeoutCount(event.EventName)

    // Continue anyway (handler may still complete in background)
}
```

**Concurrency Model (Option B: Concurrent)**
- Find all matching subscriptions (while RLocked)
- Release lock immediately
- Spawn goroutine per handler (concurrent execution)
- Track each goroutine in `inFlightWg`
- Publish returns immediately (async)

**Benefits:**
- High concurrency: multiple publishers can run simultaneously
- Multiple handlers for same event run in parallel
- Lock held briefly (only during subscription lookup)
- Safe unsubscribe during publish (copy-on-read)

**Error Handling (Option A: Recover & Continue)**
- Handler panics caught with defer/recover
- Other handlers still execute (not blocked)
- Fallback handler runs if available
- Panics logged and metriced (operator aware)
- Publish never fails due to handler error

---

## Sub-Module: Graceful Shutdown

### Close Method

```go
func (b *ChannelBus[T]) Close(ctx context.Context) error {
    // Phase 1: Stop accepting new publishes
    b.mu.Lock()
    b.closed = true
    b.mu.Unlock()

    // Phase 2: Wait for in-flight handlers to complete
    // (Option A: wait for all, no timeout)
    done := make(chan struct{})

    go func() {
        b.inFlightWg.Wait()  // Block until all handlers done
        close(done)
    }()

    select {
    case <-done:
        // All handlers completed
        return nil

    case <-ctx.Done():
        // Context deadline exceeded
        // Some handlers may still be running
        return ctx.Err()
    }
}
```

**Shutdown Sequence (Option A: Wait for All In-Flight)**

```
1. Mark bus as closed
   - New Publish() calls return ErrBusClosed
   - In-flight publishes continue

2. Wait for all handler goroutines to complete
   - inFlightWg.Wait() blocks until all handlers done
   - Handlers may take time (timeouts expire, I/O completes)

3. Return when all handlers finished
   - Or return error if ctx.Deadline exceeded
```

**Example Timeline:**

```
Close(ctx) called at T=0
  ↓
T=0: Mark bus.closed = true
T=0: Start waiting for inFlightWg
T=0.1s: Handler 1 finishes → Done()
T=0.5s: Handler 2 panics, fallback runs
T=0.7s: Handler 2 finishes → Done()
T=0.7s: Last handler done
T=0.7s: inFlightWg.Wait() returns
T=0.7s: Close() returns nil
```

**If ctx.Deadline exceeded:**

```
Close(ctx) with 1s timeout called at T=0
  ↓
T=0: Mark bus.closed = true
T=0.5s: Handler 1 times out (logs and continues)
T=1.0s: ctx.Deadline exceeded
T=1.0s: Close() returns context.DeadlineExceeded
        (Handlers 1, 2, 3 may still be running)

Cleanup:
  - Program may still wait for handlers in background (graceful)
  - Or force kill (if operator decides)
  - No handlers lost, events already durable
```

**Invariants:**
- Publish never fails after Close() is called (returns ErrBusClosed immediately)
- Handlers already dispatched continue execution
- Close waits for all handlers to finish (respecting ctx.Deadline)
- Safe to call Close multiple times (second call succeeds immediately)

---

## Concurrency & Thread Safety

### RWLock Strategy (Option C)

**Design Decision: RWLock allows concurrent publishes, exclusive access for subscribe/unsubscribe.**

```go
// Pattern:
// Publish: b.mu.RLock() → find subscriptions → RUnlock() → execute
// Subscribe: b.mu.Lock() → add subscription → Unlock()
// Unsubscribe: b.mu.Lock() → remove subscription → Unlock()
```

**Timeline Example: Concurrent Publishes**

```
Publisher A: RLock (acquire read lock)
Publisher B: RLock (also acquire read lock, allowed!)
  ↓
Both publishers iterate subscriptions concurrently
  ↓
Publisher A: RUnlock
Publisher B: RUnlock
  ↓
Both execute handlers in parallel
```

**Timeline Example: Publish During Subscribe**

```
Publisher A: RLock
  ↓
Subscriber B: Lock (waits, blocked by Publisher A's RLock)
  ↓
Publisher A: RUnlock
  ↓
Subscriber B: Lock (acquired)
Subscriber B: Add subscription
Subscriber B: Unlock
```

**Benefits:**
- Multiple publishers can run in parallel (high throughput)
- Subscribe/Unsubscribe are exclusive (safe state mutations)
- No publish blocked by subscribe/unsubscribe
- Lock held briefly (only during snapshot copy)

---

## Pattern Matching Examples

### Exact Matches (Fast Path)

```
Subscription 1: pattern = "OrderPlaced"
Subscription 2: pattern = "OrderCancelled"

Publish: Event{EventName: "OrderPlaced"}
  ↓
Check Subscription 1: "OrderPlaced" == "OrderPlaced" → MATCH (O(1) string compare)
Check Subscription 2: "OrderPlaced" == "OrderCancelled" → NO MATCH
  ↓
Handler 1 executed
```

### Regex Matches (Lazy Compilation)

```
Subscription 1: pattern = "^Order.*"
Subscription 2: pattern = "^Payment.*"

Publish: Event{EventName: "OrderCreated"}
  ↓
Check Subscription 1: "OrderCreated" == "^Order.*" → NOT EXACT
  Compile regex (first time): regexp.Compile("^Order.*")
  Cache compiled regex
  Match "OrderCreated" against compiled regex → MATCH

Check Subscription 2: "OrderCreated" == "^Payment.*" → NOT EXACT
  Compile regex (first time): regexp.Compile("^Payment.*")
  Cache compiled regex
  Match "OrderCreated" against compiled regex → NO MATCH
  ↓
Handler 1 executed

Next Publish: Event{EventName: "OrderShipped"}
  ↓
Check Subscription 1: "OrderShipped" == "^Order.*" → NOT EXACT
  Regex already cached
  Match "OrderShipped" against cached regex → MATCH (fast)
```

---

## Memory & Performance Considerations

### Subscription Overhead

```
Per subscription:
  - subscription[T] struct: ~150 bytes
  - Subscription ID string: ~10 bytes
  - Pattern string: 10-50 bytes
  - Handler function pointer: 8 bytes
  - Total: ~200 bytes per subscription

Example: 10,000 subscriptions
  ~2 MB memory overhead
  Pattern compilation cache: 100-500 KB (if 100-500 unique patterns)
```

### Publish Latency

**Warm Path (all patterns cached):**
- RLock acquire/release: ~100 ns
- Iterate subscriptions: O(n) where n = num subscriptions
  - Per subscription: exact match check (string compare) or cached regex match
  - ~1-10 µs per subscription
- Spawn goroutines: ~1-10 µs per handler
- Total: <1 ms for typical deployments (100-1000 subscriptions)

**Cold Path (first publish with new regex pattern):**
- Same as warm path, but regex compilation added
- regexp.Compile() cost: 10-100 µs (depends on pattern complexity)
- Total: 1-5 ms first time, <1 ms subsequent

**Handler Execution (concurrent):**
- Main publish thread returns immediately
- Handlers run in background goroutines
- Timeout overhead: ~1-10 µs per handler (time.After allocation)

### Goroutine Overhead

```
Scenario: Publish to 100 matching handlers

Goroutine count: 100 (one per handler)
Memory per goroutine: ~2 KB
Total: 200 KB goroutine memory

Duration: Handlers complete in 10-100 ms
  After completion: goroutines exit, WaitGroup.Done() called
  Memory reclaimed by GC
```

---

## Known Gotchas

### 1. Publish Returns Immediately (Async)

```go
// Wrong: Expecting handlers to run before Publish returns
event := Event{...}
bus.Publish(ctx, event)
// Handlers NOT guaranteed to run yet!
// Handlers run concurrently in background

// Right: If you need to wait for handlers
bus.Publish(ctx, event)
// Handlers run async
// At shutdown: Close(ctx) waits for all handlers
```

### 2. Handler Panics Don't Propagate

```go
// Handler 1 panics
Subscribe("OrderPlaced", func(e) { panic("oops") })

// Handler 2 still runs (panic recovered)
Subscribe("OrderPlaced", func(e) { /* Still executed */ })

// Publish returns nil (no error)
err := bus.Publish(ctx, event)  // err is nil, not panic
```

**Mitigation:** Check logs/metrics for handler panics.

### 3. Pattern Compilation Errors Are Silent

```go
// Bad regex pattern
Subscribe("^Order[", handler)  // Invalid regex!

// No error at subscription time (validation deferred)

// Later, first publish to similar event name
bus.Publish(ctx, Event{EventName: "OrderPlaced"})
// Pattern match fails silently (treated as no match)
// Handler never runs, no error reported
```

**Mitigation:** Validate patterns at subscription time (optional optimization).

### 4. Timeout Precision

```go
// Handler timeout is best-effort, not guaranteed
timeout := 100 * time.Millisecond
Subscribe("OrderPlaced", handler, WithHandlerTimeout(timeout))

// If handler takes 150ms:
// - Timeout fires at 100ms
// - onHandlerTimeout() called, metric recorded
// - Handler continues running in background
// - Handler completes at 150ms
// - goroutine exits, WaitGroup.Done() called

// Handler may complete before or after timeout
```

### 5. Handler Context Parameter

Handlers receive `Event[T]` only, **not** a context parameter. This is by design.

```go
// Handler signature
Subscribe("OrderPlaced", func(e Event[T]) {
    // e.Event, e.PreviousEvent available
    // NO context parameter
})
```

**Why no context?**
- Bus.Publish() is async — caller's context deadline is irrelevant to handler
- If handlers could check context.Done(), they'd expect cancellation (but don't get it)
- Handlers should be fire-and-forget with no connection to caller's deadline
- Trace IDs/request IDs are in the Event envelope if needed (developer choice)

**If you need context in handlers:**
- Store trace ID in Event[T] (application concern)
- Use a separate context channel if handler must be cancellable (advanced)
- Use handler timeout (WithHandlerTimeout) instead of context deadline

### 6. Close Blocks If Handlers Hang

```go
// Handler infinite loops
Subscribe("OrderPlaced", func(e) {
    for { /* infinite loop */ }
})

// Later, try to shutdown
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
err := bus.Close(ctx)
// Blocks for 5 seconds waiting for handler
// Returns context.DeadlineExceeded

// Operator must decide: wait longer, force kill, or both
```

---

## Error Handling

### Publish Error Semantics

`Publish()` **only returns synchronous errors** (errors that occur before handlers are spawned). Handler errors are **not propagated** because handlers execute asynchronously in background goroutines.

```go
func (b *ChannelBus[T]) Publish(ctx context.Context, event Event[T]) error {
    // Only these errors are returned:
    // - ErrBusClosed (bus is closed, no new publishes)

    // These errors are NOT returned (async):
    // - Handler panics (caught, logged, fallback runs)
    // - Handler timeouts (logged, continue)
    // - Pattern match failures (logged)

    return nil  // Only if successful or no matching handlers
}
```

**Synchronous errors (returned to caller):**

| Scenario | Error | Behavior |
|----------|-------|----------|
| Subscribe with nil handler | ErrNilHandler | Return error, no subscription |
| Subscribe with empty pattern | ErrEmptyPattern | Return error, no subscription |
| Publish after Close | ErrBusClosed | Return error immediately |

**Asynchronous errors (not returned, logged instead):**

| Scenario | Handling | Visibility |
|----------|----------|----------|
| Handler panics | Caught via defer/recover | Logged to stderr; fallback handler runs if available |
| Handler timeout | Select expires | Logged to stderr |
| Pattern regex fails | Silent | Handler doesn't run for that subscription |

**Design Rationale:**

Since handlers run asynchronously in background goroutines, errors cannot be returned to the Publish() caller. The caller has already received control back. This is intentional: Asynx guarantees that `Send()` succeeds iff the event is durable in the eventstore. Bus dispatch (which is async) is decoupled from that durability guarantee.

---

## Configuration

### Subscription Options

```go
// Fallback handler (optional)
WithFallback(func(Event[T]) {
    // Runs if primary handler panics
})

// Handler timeout (optional)
WithHandlerTimeout(100 * time.Millisecond)

// Example
id, err := bus.Subscribe("OrderPlaced",
    primaryHandler,
    WithFallback(fallbackHandler),
    WithHandlerTimeout(5 * time.Second),
)
```

---

## Summary

The **ChannelBus[T]** is a high-concurrency, fault-tolerant event dispatcher:

- **Lazy pattern compilation** — Patterns compiled on first use, cached
- **RWLock synchronization** — Concurrent publishes, exclusive subscription changes
- **Concurrent handler execution** — Handlers run in parallel goroutines
- **Recover & continue** — Handler panics don't block other handlers
- **Graceful shutdown** — Waits for all handlers to complete

**Thread-Safe:** Multiple publishers and subscribers can operate concurrently.

**Fault-Tolerant:** Handler panics, timeouts, and errors are logged but don't affect other handlers.

**Async:** Publish returns immediately; handlers run in background.

**Bounded Resources:** Subscriptions and pattern cache are bounded; goroutine cleanup is automatic.

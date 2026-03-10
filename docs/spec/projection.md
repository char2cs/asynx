# Projection Package Specification

## Overview

The `projection` package implements the subscription system for building eventually-consistent read models. Projections are developer-facing callbacks that fire after events are durably committed to the eventstore. They are the mechanism for:

- Building read models (denormalized views across aggregates)
- Publishing events to external systems
- Triggering side effects (emails, webhooks, etc.)
- Synchronizing with other systems

Projections are not Asynx's responsibility — the event is already safe. If a projection callback fails, the developer uses `Replay()` to recover. Asynx provides structure (fallback handlers, timeout configuration, panic recovery) but the behavior is up to the developer.

Projections depend on `core` and `bus` to dispatch events.

---

## Core Subscription Model

### Public API

```go
// Subscribe registers a primary handler (required) and optional fallback handler
Subscribe(pattern string, primaryHandler func(Event[T]), opts ...SubscriptionOpt) (string, error)

// Unsubscribe removes a subscription by ID
Unsubscribe(id string) error

// Subscription options:
WithFallback(handler func(Event[T])) SubscriptionOpt
WithHandlerTimeout(timeout time.Duration) SubscriptionOpt
```

### Method: `Subscribe`

**Signature**
```go
Subscribe(pattern string, primaryHandler func(Event[T]), opts ...SubscriptionOpt) (string, error)
```

**Purpose**
Registers a callback function to receive events matching the pattern. Supports optional fallback handler and timeout configuration.

**Parameters**
- `pattern` — exact event name (e.g., `"OrderPlaced"`) or regex pattern (e.g., `"^Order.*"`)
  - Matched against event's `EventName` field
  - Exact matches are preferred to regex (faster)
- `primaryHandler` — the main callback function
  - Receives Event[T] after event is committed
  - Called synchronously (in default in-process bus)
  - Panics are recovered by Asynx
  - If panics or times out, fallback fires (if configured)
- `opts` — optional subscription configuration (fallback, timeout)

**Return Values**
- `string` — unique subscription ID, non-empty
  - Used to unsubscribe later
  - ID format is implementation-specific
- `error` — non-nil if subscription failed
  - Invalid regex pattern → error
  - Invalid configuration → error

**Invariants**
- **Subscription ID is unique** — no two active subscriptions have the same ID
- **Handler sees only future events** — subscriptions don't replay past events (unless manually via Replay)
- **No guaranteed ordering** — multiple subscriptions may fire out of order (depends on bus implementation)

**Side Effects**
- Registers callback with the bus
- Allocates memory for pattern and handler
- Future matching events trigger this handler

**Example: Simple Subscription**
```go
id, err := instance.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Handle OrderPlaced event:
    // - Update read model (cache, database)
    // - Send notification email
    // - Publish to another system
    // - Etc.
    log.Printf("Order %s placed for customer %s\n", e.AggregateID, e.Aggregate.CustomerID)

    // If this panics:
    // 1. Asynx recovers the panic
    // 2. Calls registered panic handler (if any)
    // 3. Continues to next subscription
    // 4. Does NOT trigger fallback
})

if err != nil {
    return fmt.Errorf("subscribe failed: %w", err)
}

// Store ID for later unsubscribe (if needed)
defer instance.Unsubscribe(id)
```

---

### Method: `Unsubscribe`

**Signature**
```go
Unsubscribe(id string) error
```

**Purpose**
Removes a subscription by its ID. After unsubscribe, the handler will not be called for new events.

**Parameters**
- `id` — subscription ID returned from Subscribe (non-empty string)

**Return Values**
- `error` — non-nil if unsubscribe failed
  - ID doesn't exist → error (or nil, implementation-dependent)

**Invariants**
- **Handler never called after unsubscribe** — subscription is fully removed
- **In-flight handlers not interrupted** — if handler is executing when unsubscribe is called, it finishes

**Side Effects**
- Removes subscription from the bus
- Frees associated memory

**Error Handling**
- Non-existent ID → error or nil
- Idempotent behavior varies (call twice with same ID may error the second time)

**Example**
```go
id, _ := instance.Subscribe("OrderPlaced", handler)

// Later, stop listening:
err := instance.Unsubscribe(id)
if err != nil {
    log.Printf("unsubscribe failed: %v\n", err)
}
// Handler will no longer be called
```

---

## Subscription Options

### `WithFallback(handler func(Event[T]))`

Registers a fallback handler to be called if the primary handler fails. Triggered by:
- Primary handler panic
- Primary handler timeout (if `WithHandlerTimeout` is configured)

**Signature & Usage**
```go
id, err := instance.Subscribe("PaymentProcessed",
    primaryHandler,
    asynx.WithFallback(fallbackHandler),
)
```

**Semantics**

When primary handler fails:

```
Event dispatched
  ↓
Primary handler called
  ↓
Primary handler panics OR timeout exceeds
  ↓
Primary is considered failed
  ↓
Fallback handler called (if configured)
  ↓
Fallback executes (receives same Event[T])
  ↓
If fallback panics: panic handler called
```

**What Fallback Is NOT**
- **Not a retry** — primary ran (or tried) and failed, fallback is a different code path
- **Not a transaction** — event is already durable before either handler fires, fallback doesn't roll back
- **Not a substitute for idempotency** — developers should make both handlers safely re-enterable

**What Fallback IS**
- **A delivery reliability mechanism** — when primary can't do its job, fallback handles it
- **Panic recovery** — panics in primary don't crash dispatch; fallback gets a chance
- **Timeout handling** — long-running primaries are interrupted; fallback cleans up

**Example: Fallback for External Integration**
```go
id, _ := instance.Subscribe("PaymentProcessed",
    func(e asynx.Event[Payment]) {
        // Primary: record payment in ledger
        ledger.Record(e.Aggregate)
    },
    asynx.WithFallback(func(e asynx.Event[Payment]) {
        // Fallback: primary failed, use dead letter queue
        deadLetterQueue.Push(e)
        // Operator will manually review and retry
    }),
    asynx.WithHandlerTimeout(5 * time.Second),
)
```

---

### `WithHandlerTimeout(duration time.Duration)`

Sets a timeout for the primary handler. If handler doesn't return within the duration, it's considered failed and the fallback is triggered (if configured).

**Signature & Usage**
```go
id, err := instance.Subscribe("OrderPlaced",
    primaryHandler,
    asynx.WithHandlerTimeout(5 * time.Second),
)
```

**Behavior**

```
Primary handler called
  ↓
Timer starts (5 seconds)
  ↓
Handler executes
  ↓
Handler returns before timeout
  ↓
All good, event processing continues
```

OR:

```
Primary handler called
  ↓
Timer starts (5 seconds)
  ↓
Handler executes
  ↓
Handler still running at timeout
  ↓
Primary is considered failed
  ↓
Fallback handler called (if configured)
  ↓
Primary handler may still be running (not forcefully killed)
```

**Important:** The primary handler **is not forcefully interrupted**. It may continue running in the background. The timeout marks it as failed and triggers the fallback, but the original goroutine is left to finish.

**Use Case**
External system calls that could block forever:

```go
instance.Subscribe("UserRegistered",
    func(e asynx.Event[User]) {
        // Call external service (could hang)
        sendWelcomeEmail(e.Aggregate.Email)  // HTTP request, could hang
    },
    asynx.WithHandlerTimeout(3 * time.Second),
    asynx.WithFallback(func(e asynx.Event[User]) {
        // Service timeout, try async queue instead
        emailQueue.Enqueue(e.Aggregate.Email, "welcome")
    }),
)
```

**Default Behavior**
If `WithHandlerTimeout` is not set, only panic triggers the fallback (no timeout-based triggering).

---

## Panic Handling

### Handler Panic Recovery

If a primary handler panics:

```go
instance.Subscribe("OrderShipped", func(e asynx.Event[Order]) {
    panic("something terrible happened")
})

// Asynx:
// 1. Recovers the panic
// 2. Calls panic handler (if configured) with PanicEvent
// 3. If fallback is configured, calls fallback
// 4. Continues to next subscription
```

**Panic Handler Configuration**
```go
instance, _ := asynx.New[Order]().
    WithEventStore(store).
    WithPanicHandler(func(e asynx.PanicEvent[Order]) {
        log.Printf("projection panic on %s: %v\n", e.EventName, e.Err)
        metrics.IncrementPanicCount()
        // Could also: alert ops, send to dead letter, retry, etc.
    }).
    Build()
```

**PanicEvent Type**
```go
type PanicEvent[T any] struct {
    EventName  string              // Event that triggered the panic
    Aggregate  T                   // Current aggregate state
    Projection func(Event[T])      // The callback function that panicked
    Err        error               // Panic as error (recovered via recover())
}
```

### Panic Handler Is Also Panic-Safe

If the registered panic handler itself panics, Asynx recovers that too and silently continues. The system never crashes due to panic — everything is protected.

---

## Fallback Handler Execution

### Triggering Conditions

Fallback fires if:
1. Primary handler panics (panic not caught by primary)
2. Primary handler exceeds timeout (if `WithHandlerTimeout` is configured)

### Fallback Signature

```go
fallback := func(e asynx.Event[Order]) {
    // Receives the same Event[T] the primary would have received
    // Runs synchronously (in the same dispatch cycle)
}
```

### Fallback Behavior

```
Primary handler fails
  ↓
Fallback handler called
  ↓
Fallback receives Event[T]
  ↓
Fallback executes (may also panic, timeout, etc.)
  ↓
If fallback panics: panic handler called
  ↓
Dispatch completes (event is safe, no side effects undone)
```

### Fallback Guarantees

- **Receives the correct event** — same Event[T] the primary would have seen
- **Runs synchronously** — completes before dispatch returns
- **Panic-safe** — fallback panics are also recovered
- **Timeout-safe** — if fallback has timeout, it also respects `WithHandlerTimeout`

### Fallback Is NOT a Retry

```go
// Wrong: don't do this
asynx.Subscribe("OrderShipped",
    func(e asynx.Event[Order]) {
        // Primary: ship the order via API
        if err := shipmentAPI.Ship(e.Aggregate.OrderID) {
            // API error, return error? NO, handlers don't return errors
            // Panic and let fallback handle it? NO, this is not a retry
            panic(err)  // Only if truly unrecoverable
        }
    },
    asynx.WithFallback(func(e asynx.Event[Order]) {
        // Fallback: NOT a retry of the same call
        // It's a different strategy
        shipmentQueue.Enqueue(e.Aggregate.OrderID)
    }),
)

// Right: handle recoverable errors in the handler
asynx.Subscribe("OrderShipped",
    func(e asynx.Event[Order]) {
        // Primary: ship via API with retry logic
        var lastErr error
        for attempt := 0; attempt < 3; attempt++ {
            err := shipmentAPI.Ship(e.Aggregate.OrderID)
            if err == nil {
                return  // Success
            }
            lastErr = err
        }
        // All retries failed, give up
        // This is unrecoverable — let fallback handle
        panic(lastErr)
    },
    asynx.WithFallback(func(e asynx.Event[Order]) {
        // Fallback: primary exhausted its retries
        // Use queue instead
        shipmentQueue.Enqueue(e.Aggregate.OrderID)
    }),
)
```

---

## Replay for Recovery

If a projection callback failed (partial execution, crash mid-callback), developers use `Replay()` to re-run the projection logic.

**Usage**
```go
// Projection callback crashed while updating read model
// Replay to re-run it:
err := instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    // Re-run projection logic:
    // - Update read model
    // - Publish to external system
    // - Etc.
    readModel.UpdateOrder(e)
})

if err != nil {
    log.Printf("replay failed: %v\n", err)
}
```

**Replay Guarantees**
- **Version order** — events arrive in ascending version order
- **No gaps** — all versions between fromVersion and toVersion are included
- **Idempotent** — replay is safe to run multiple times
- **Read-only** — never triggers snapshots or state mutations in the eventstore

---

## Pattern Matching

### Exact Match

Subscribe to a single event:

```go
instance.Subscribe("OrderPlaced", func(e asynx.Event[Order]) {
    // Fires only for OrderPlaced events
})
```

### Regex Match

Subscribe to multiple related events:

```go
// Regex pattern:
instance.Subscribe("^Order(Placed|Cancelled|Shipped)$", func(e asynx.Event[Order]) {
    // Fires for OrderPlaced, OrderCancelled, OrderShipped
})

// Wildcard pattern:
instance.Subscribe(".*", func(e asynx.Event[Order]) {
    // Fires for ALL events
})
```

---

## Example: Multi-Tier Projection

```go
// Scenario: Financial transaction projection with high reliability
// Primary handler calls external ledger
// Fallback handler queues for retry

id, err := instance.Subscribe("TransactionApproved",
    func(e asynx.Event[Transaction]) {
        // Primary: record in ledger immediately
        if err := ledger.RecordDebit(
            e.Aggregate.Amount,
            e.Aggregate.Account,
            e.Aggregate.Reference,
        ); err != nil {
            // Unrecoverable error — panic and let fallback handle
            panic(fmt.Sprintf("ledger error: %v", err))
        }

        // Update read model
        transactionCache.Store(e.AggregateID, e.Aggregate)
    },

    asynx.WithFallback(func(e asynx.Event[Transaction]) {
        // Fallback: ledger was unavailable or slow
        // Use queue for later processing
        transactionQueue.Enqueue(e.Aggregate)

        // Send alert to ops
        alerting.NotifyOpsTeam(
            "transaction ledger unavailable",
            e.Aggregate.ID,
        )
    }),

    asynx.WithHandlerTimeout(5 * time.Second),
)

if err != nil {
    log.Fatalf("subscribe failed: %v\n", err)
}

// Later, if needed, unsubscribe:
instance.Unsubscribe(id)
```

---

## Example: Audit Logging

```go
// Simple audit projection: log all events
instance.Subscribe(".*", func(e asynx.Event[Order]) {
    auditLog.Record(e.EventName, e.AggregateID, e.Version, e.OccurredAt)
})

// No fallback needed — audit log is fire-and-forget
// No timeout needed — local write is fast
```

---

## Example: Cross-Aggregate Read Model

```go
// Build a customer dashboard: all orders for a customer
instance.Subscribe("^Order.*", func(e asynx.Event[Order]) {
    // Event contains order state
    customerID := e.Aggregate.CustomerID
    orderID := e.AggregateID

    // Update read model (denormalized view):
    dashboard.UpdateCustomerOrders(customerID, orderID, e.Aggregate)
})

// Now a REST API can query the read model:
// GET /customers/{id}/orders → returns all orders for that customer
// (no cross-aggregate loading, just a lookup in the denormalized view)
```

---

## Implementation Requirements

### Thread Safety
- Subscribe, Unsubscribe, Replay must be safe for concurrent calls
- Handlers may be called concurrently (from different aggregates) in high-throughput scenarios

### Handler Isolation
- Panics in one handler must not affect others
- Timeouts in one handler must not affect others

### Pattern Compilation
- Regex patterns should be compiled once at subscribe time, not per-event

### Cleanup
- Unsubscribe must free all associated memory
- Close must wait for in-flight handlers

---

## Error Handling

**Subscribe errors:**
- Invalid regex pattern → return error
- Invalid configuration → return error
- Other failures → return error

**Unsubscribe errors:**
- Non-existent ID → error or nil (implementation-dependent)

**Callback errors:**
- Panics → recovered and passed to panic handler
- Timeouts → marked as failed, fallback fires

**Replay errors:**
- Aggregate not found → ErrNotFound
- Version range invalid → error
- Storage error → propagate

---

## Known Limitations

**No guaranteed delivery.** The default in-process bus offers no durability guarantees. Events published to handlers that crash are lost. Use external buses for guaranteed delivery.

**No guaranteed ordering across aggregates.** Events from different aggregates may dispatch out of order. Causality across aggregates is not preserved.

**No atomic cross-aggregate updates.** Projections updating multiple read models must handle partial failures and eventual consistency.

**Synchronous dispatch can block.** Long-running handlers block the processor. For asynchronous dispatch, use an external bus implementation.

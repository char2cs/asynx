# Core Package Specification

## Overview

The `core` package is the foundational layer of Asynx. It defines all shared types, interfaces, and contracts that every other package builds upon. Core has zero internal dependencies — it is imported by everything else, and depends on nothing. The builder lives here as the assembly point for Asynx's pluggable architecture: it knows all interfaces but no implementations.

Core exists to keep a minimal, stable set of types that implementations can rely on without circular dependencies. If a type lives in `core`, every module can use it. Core owns event schemas, type constraints, and the builder pattern.

## Types & Interfaces

### `Event[T any]`

The central event envelope in Asynx. Every event published to the bus and written to the eventstore uses this type. It carries both old and new aggregate state — the CDC diff is computed internally for storage and never exposed through this type.

```go
type Event[T any] struct {
    ID                string    // Unique event identifier, assigned by processor
    AggregateID       string    // The aggregate this event belongs to
    EventName         string    // Event name from command.EventName() (e.g., "PackageAdded")
    Version           int64     // Monotonically increasing version for this aggregate
    SchemaVersion     int       // Schema version this event was written at (stamped by processor)
    OccurredAt        time.Time // Timestamp when event was created
    Aggregate         T         // Current aggregate state (the new state after this event)
    PreviousAggregate T         // Previous aggregate state (before this event was applied)
}
```

#### Invariants
- **ID is unique** across all events — no two events have the same ID
- **AggregateID is non-empty** — every event belongs to an aggregate
- **EventName is non-empty** — must match the event name returned by the command that produced it
- **Version is monotonically increasing** per aggregate — version N+1 always follows version N for the same aggregate
- **Version is gap-free** per aggregate — there are no missing versions in the stream (1, 2, 3... never 1, 3, 5)
- **SchemaVersion is stable** — once written, never changes; indicates the schema version at write time
- **OccurredAt is set** — never zero; timestamp of event creation
- **Aggregate and PreviousAggregate are always set** — both old and new state are present for every event (even on first event, PreviousAggregate is the zero value of T)

#### Role in the System
- **For the bus**: Events are dispatched to projection callbacks for eventual-consistent read models
- **For the eventstore**: Events are written to durable storage as RFC 6902 diffs
- **For the developer**: The Event[T] received in projection callbacks contains full old and new state for decision-making

#### What Event[T] Does NOT Contain
- The RFC 6902 diff — only old and new full state (diff is internal to eventstore)
- Causality/causal ordering — events are ordered by version, not by causality
- Correlation IDs — the developer owns tracking across events
- Custom metadata — add it to the aggregate itself or pass it separately

---

### `Bus[T any]` Interface

The event dispatcher contract. Asynx publishes committed events to the bus, and the bus routes them to subscribed projection callbacks. The bus is pluggable — the default is in-process channels, but developers can swap in external message brokers (Kafka, NATS, Redis, etc.) for multi-node deployments.

```go
type Bus[T any] interface {
    // Publish sends an event to all subscribers matching the event's EventName.
    // Called after the event is durably written to the eventstore.
    // Returns error only if publication failed — the event is already safe in storage.
    Publish(ctx context.Context, event Event[T]) error

    // Subscribe registers a handler function for events matching the pattern.
    // pattern supports exact event names (e.g., "PackageAdded") or regex patterns (e.g., "^Package.*").
    // Returns a subscription ID for later unsubscription, or error if subscription failed.
    Subscribe(pattern string, handler func(Event[T])) (string, error)

    // Unsubscribe removes a subscription by its ID.
    // No error if subscription ID doesn't exist (idempotent).
    Unsubscribe(id string) error

    // Close gracefully shuts down the bus.
    // Waits for in-flight events to finish being dispatched.
    // Returns error if shutdown times out or fails.
    Close(ctx context.Context) error
}
```

#### Method: `Publish`

**Signature**
```go
Publish(ctx context.Context, event Event[T]) error
```

**Purpose**
Called by the processor after every event is durably committed to the eventstore. Dispatches the event to all subscriptions that match the event's `EventName`.

**Parameters**
- `ctx` — context for cancellation and timeouts; respects caller's deadline
- `event` — the committed event, fully populated (ID, Version, Aggregate, PreviousAggregate all set)

**Return Values**
- `error` — non-nil if publication failed (e.g., network error in external broker)
  - **Critical**: A non-nil error means dispatch failed, NOT that the event was lost
  - The event is already safely written to the eventstore before `Publish` is called
  - The error only signals a delivery problem to subscribers downstream

**Invariants**
- **Event is already durable** — publish is called only after eventstore write succeeds
- **All subscribers matching the pattern get the event** — or none do if publish fails entirely
- **Failure is not idempotent** — if publish returns error, it's unclear if some subscribers received it; calling send again is incorrect

**Side Effects**
- Calls matching subscription handlers
- Handler panics are recovered and passed to the panic handler, not propagated up
- If handler blocks, publish blocks until it returns (synchronous dispatch in default in-process bus)

**Error Handling**
- Network errors from external brokers (Kafka, NATS) surface as errors
- Handler panics don't surface as errors to Publish — they're caught and handled separately
- Context timeout returns the context error

**Example**
```go
// After a command succeeds and is written to eventstore:
event := Event[Order]{
    ID:                "evt_12345",
    AggregateID:       "order_999",
    EventName:         "OrderPlaced",
    Version:           1,
    SchemaVersion:     1,
    OccurredAt:        time.Now(),
    Aggregate:         newOrderState,
    PreviousAggregate: *new(Order),  // zero value, first event
}

// Processor calls:
err := bus.Publish(ctx, event)
if err != nil {
    // Event is safe in eventstore, but dispatch failed
    // Caller must decide: retry, alert, dead letter, etc.
}
```

---

#### Method: `Subscribe`

**Signature**
```go
Subscribe(pattern string, handler func(Event[T])) (string, error)
```

**Purpose**
Registers a callback function to receive events matching a pattern. The pattern is matched against the `EventName` field of events. Multiple handlers can be registered for the same pattern.

**Parameters**
- `pattern` — exact event name (e.g., `"OrderPlaced"`) or regex pattern (e.g., `"^Order.*"`)
  - Regex patterns are evaluated as full-string matches (anchored at both ends)
  - Exact matches are preferred to regex matches (cheaper evaluation)
- `handler` — the callback function, receives Event[T] after publish
  - Must be non-nil
  - Panics inside the handler are recovered; configure a panic handler via WithPanicHandler to be notified

**Return Values**
- `string` — subscription ID, non-empty, unique for the lifetime of this subscription
  - Used to unsubscribe later
  - ID format is implementation-specific (could be UUID, counter, etc.)
- `error` — non-nil if subscription failed (e.g., invalid regex pattern, implementation limits reached)

**Invariants**
- **Subscription ID is unique** — no two active subscriptions have the same ID
- **Pattern is immutable** — the pattern used to match is locked at subscribe time
- **Handler is called synchronously** — event dispatch waits for handler to return
- **Handler sees all matching events** — from subscribe time forward (no replay of past events)

**Side Effects**
- Adds a subscription to the bus
- Memory is allocated to store the handler and pattern
- Subsequent publish calls invoke this handler for matching events

**Error Handling**
- Regex compile error returns error
- If pattern is invalid regex, error is returned
- Empty pattern or nil handler should error (implementation-dependent validation)

**Example**
```go
// Subscribe to a single event
id1, err := bus.Subscribe("OrderPlaced", func(e Event[Order]) {
    // handle OrderPlaced events
    log.Printf("Order %s placed\n", e.AggregateID)
})

// Subscribe to multiple events with regex
id2, err := bus.Subscribe("^Order(Placed|Cancelled|Shipped)$", func(e Event[Order]) {
    // handle any of those three events
    updateReadModel(e)
})

// Keep subscription ID for later unsubscription
defer bus.Unsubscribe(id1)
defer bus.Unsubscribe(id2)
```

---

#### Method: `Unsubscribe`

**Signature**
```go
Unsubscribe(id string) error
```

**Purpose**
Removes a subscription by its ID. After unsubscribe, the handler will not be called for new events.

**Parameters**
- `id` — subscription ID returned from Subscribe (non-empty string)

**Return Values**
- `error` — non-nil if unsubscribe failed (typically only if id doesn't exist)
  - Unsubscribe is idempotent in the default implementation: calling twice with the same ID returns error the second time (id already gone) or succeeds silently

**Invariants**
- **Handler never called after unsubscribe** — subscription is fully removed
- **Resources are freed** — handler and pattern memory are released
- **Unsubscribe is not a blocking operation** — completes immediately

**Side Effects**
- Removes subscription from the bus
- Frees associated memory
- In-flight handlers (already dispatching) are not interrupted

**Error Handling**
- If subscription ID doesn't exist, returns error (or nil, implementation-dependent)
- Concurrent unsubscribe of same ID may race; second call may fail or succeed

**Example**
```go
subID, _ := bus.Subscribe("OrderPlaced", handler)

// Later, stop listening:
err := bus.Unsubscribe(subID)
if err != nil {
    log.Printf("unsubscribe failed: %v\n", err)
}
```

---

#### Method: `Close`

**Signature**
```go
Close(ctx context.Context) error
```

**Purpose**
Gracefully shuts down the bus. Stops accepting new subscriptions and publications, waits for in-flight event dispatches to finish.

**Parameters**
- `ctx` — context for timeout and cancellation; used to set an upper bound on shutdown time

**Return Values**
- `error` — non-nil if shutdown times out, fails, or is cancelled
  - Timeout error if in-flight events don't finish within deadline
  - Context cancelled error if caller cancels the context

**Invariants**
- **No new publications accepted after Close** — Publish returns error if called after Close
- **In-flight events complete** — handlers currently running are allowed to finish (up to deadline)
- **All subscriptions are cleaned up** — all handlers are unregistered after Close

**Side Effects**
- Bus is marked as closed/inactive
- Subscriptions are cleared
- Resources (channels, connections, etc.) are released
- Subsequent Publish, Subscribe, Unsubscribe calls are errors

**Error Handling**
- Context timeout returns deadline exceeded error
- If handlers hang and timeout fires, those handlers may be force-killed (implementation detail)
- Calling Close multiple times may error the second time (idempotent or not, implementation-dependent)

**Example**
```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err := bus.Close(ctx)
if err != nil {
    log.Printf("bus shutdown failed: %v\n", err)
    // Could be timeout, handlers hung, etc.
}
```

---

### `Builder` Type

The assembly point for configuring Asynx instances. Uses a fluent builder pattern to collect infrastructure implementations and behavioral options, then builds a fully-configured instance.

The builder is defined in `core` because it knows all interfaces but no implementations. Concrete implementations (channel bus, memory store, etc.) live in other packages.

#### Builder Signature

```go
type Builder[T any] struct {
    // private fields
}

// Create a new builder for aggregate type T
func New[T any]() *Builder[T]

// Set infrastructure (required or optional with defaults)
func (b *Builder[T]) WithEventStore(s Store) *Builder[T]
func (b *Builder[T]) WithSnapshotStore(s Store) *Builder[T]
func (b *Builder[T]) WithBus(bus Bus[T]) *Builder[T]

// Set behavioral options (optional)
func (b *Builder[T]) WithShardingOpts(opts ShardingOpts) *Builder[T]
func (b *Builder[T]) WithSchemaVersion(v int) *Builder[T]
func (b *Builder[T]) WithUpcaster(fromVersion int, fn func(eventName string, raw []byte) []byte) *Builder[T]
func (b *Builder[T]) WithPanicHandler(fn func(PanicEvent[T])) *Builder[T]

// Build the instance
func (b *Builder[T]) Build() (*Instance[T], error)
```

#### Method: `New[T any]()`

**Purpose**
Creates a new builder for the given aggregate type T.

**Return Values**
- `*Builder[T]` — empty builder, ready for configuration

**Example**
```go
builder := asynx.New[Order]()
```

---

#### Method: `WithEventStore(s Store)`

**Purpose**
Sets the event store implementation. This is the only **required** configuration — Build() returns error if missing.

**Parameters**
- `s` — non-nil Store implementation

**Invariants**
- Event store must be set before Build()
- Cannot be set to nil

**Chaining**
Returns `*Builder[T]` to allow method chaining.

**Example**
```go
asynx.New[Order]().
    WithEventStore(myPostgresStore).
    WithSnapshotStore(myRedisStore).
    Build()
```

---

#### Method: `WithSnapshotStore(s Store)`

**Purpose**
Sets the snapshot store implementation. Optional — defaults to the same store as WithEventStore if not provided.

**Parameters**
- `s` — non-nil Store implementation

**Default Behavior**
If not called, snapshots are written to the same store as events.

**Use Case**
Developers can route snapshots to fast infrastructure (Redis) and events to durable infrastructure (Postgres) for performance optimization.

**Example**
```go
asynx.New[Order]().
    WithEventStore(postgresStore).           // durable, slower
    WithSnapshotStore(redisStore).           // fast, optional durability
    Build()
```

---

#### Method: `WithBus(bus Bus[T])`

**Purpose**
Sets the event dispatcher implementation. Optional — defaults to in-process channel bus if not provided.

**Parameters**
- `bus` — non-nil Bus[T] implementation

**Default Behavior**
If not called, an in-process channel bus is used. Events are published only to subscribers on the same process.

**Use Case**
Multi-node deployments swap the default for an external broker (Kafka, NATS, Redis Streams) to fan events across all nodes.

**Example**
```go
asynx.New[Order]().
    WithEventStore(eventStore).
    WithBus(kafkaBus).  // external multi-node bus
    Build()
```

---

#### Method: `WithShardingOpts(opts ShardingOpts)`

**Purpose**
Configures the processor's sharded worker pool.

**Parameters**
```go
type ShardingOpts struct {
    Shards     int // Number of shard queues, default 8
    QueueDepth int // Max commands per shard (0 = unbounded, default 0)
}
```

**Defaults**
- `Shards: 8` — 8 parallel worker pools
- `QueueDepth: 0` — unbounded queue per shard

**Use Case**
- Increase shards for high-concurrency workloads
- Set QueueDepth to cap in-memory buffering and get `ErrQueueFull` backpressure when overloaded

**Example**
```go
asynx.New[Order]().
    WithEventStore(eventStore).
    WithShardingOpts(asynx.ShardingOpts{
        Shards:     16,  // more parallelism
        QueueDepth: 1000, // reject if queue hits 1000 cmds/shard
    }).
    Build()
```

---

#### Method: `WithSchemaVersion(v int)`

**Purpose**
Sets the current schema version for this instance. Defaults to `1`. Every event written by this instance is stamped with this version.

**Parameters**
- `v` — schema version integer, typically 1-based

**Invariants**
- SchemaVersion must be >= 1
- Once an instance is created, changing the schema version requires a new instance

**Use Case**
When the aggregate struct changes destructively (rename field, remove field, change type), bump the schema version and register upcasters to migrate old events forward.

**Example**
```go
asynx.New[Order]().
    WithEventStore(eventStore).
    WithSchemaVersion(3).
    WithUpcaster(1, upcaster1To2).
    WithUpcaster(2, upcaster2To3).
    Build()
```

---

#### Method: `WithUpcaster(fromVersion int, fn func(eventName string, raw []byte) []byte)`

**Purpose**
Registers a schema migration function for a version transition. Used when replaying old events that were written at a lower schema version.

**Parameters**
- `fromVersion` — the schema version of the stored event being migrated
- `fn` — migration function that receives the event name and raw RFC 6902 patch bytes, returns corrected bytes

**Behavior**
When an event stored at version 1 is replayed on a version 3 instance:
1. Upcaster(1) is applied → produces bytes for version 2
2. Upcaster(2) is applied → produces bytes for version 3
3. Final bytes are applied to the aggregate state

**Invariants**
- Upcasters are applied in version order
- Each upcaster is focused on one transition only
- No if-statements checking versions inside the function — that's handled by the framework

**Example**
```go
asynx.New[Order]().
    WithEventStore(eventStore).
    WithSchemaVersion(3).
    WithUpcaster(1, func(eventName string, raw []byte) []byte {
        // Fix v1 → v2: rename "/status" → "/state"
        return bytes.ReplaceAll(raw,
            []byte(`"/status"`),
            []byte(`"/state"`))
    }).
    WithUpcaster(2, func(eventName string, raw []byte) []byte {
        // Fix v2 → v3: rename "/state" → "/statusCode"
        return bytes.ReplaceAll(raw,
            []byte(`"/state"`),
            []byte(`"/statusCode"`))
    }).
    Build()
```

---

#### Method: `WithPanicHandler(fn func(PanicEvent[T]))`

**Purpose**
Registers a panic handler to be called when a projection callback panics.

**Parameters**
```go
type PanicEvent[T any] struct {
    EventName  string                  // Event that triggered the panic
    Aggregate  T                        // Current aggregate state
    Projection func(Event[T])          // The callback function that panicked
    Err        error                   // Normalized panic as error
}
```

**Behavior**
- Called when a primary projection handler panics (if no fallback) or when a fallback also panics
- Called after Asynx internally recovers the panic
- Does not block event publication — the panic handler is called asynchronously

**Default Behavior**
If not provided, panics are silently recovered and execution continues.

**Example**
```go
asynx.New[Order]().
    WithEventStore(eventStore).
    WithPanicHandler(func(e asynx.PanicEvent[Order]) {
        log.Printf("projection panic on %s: %v\n", e.EventName, e.Err)
        metrics.IncrementPanicCount()
        // Could also send to a dead letter queue, alert ops, etc.
    }).
    Build()
```

---

#### Method: `Build()`

**Signature**
```go
func (b *Builder[T]) Build() (*Instance[T], error)
```

**Purpose**
Validates all required fields and builds the fully-configured Asynx instance.

**Return Values**
- `*Instance[T]` — ready-to-use Asynx instance
- `error` — non-nil if build failed (e.g., missing event store)

**Validation Rules**
- `WithEventStore` is required — Build() returns error if missing
- All other methods are optional with sensible defaults
- Regex patterns in upcasters are not validated (regex is compiled lazily during event replay)

**Error Cases**
- Missing event store: `ErrMissingEventStore` or similar
- Invalid configuration: specific error describing the problem

**Example**
```go
instance, err := asynx.New[Order]().
    WithEventStore(postgresStore).
    WithBus(kafkaBus).
    WithSchemaVersion(2).
    WithUpcaster(1, migrateV1toV2).
    Build()

if err != nil {
    log.Fatalf("failed to build asynx: %v\n", err)
}

// instance is now ready to use:
// instance.Send(ctx, cmd)
// instance.Subscribe(pattern, handler)
// instance.Get(ctx, aggregateID)
// instance.Shutdown(ctx)
```

---

## Implementation Requirements

### For Implementations of Bus[T]

1. **Thread-safe operations** — Publish, Subscribe, Unsubscribe, Close must be safe for concurrent calls
2. **Handler isolation** — panics in one handler must not affect others
3. **Pattern matching** — exact names and regex patterns must both be supported
4. **No event replay** — subscriptions only receive events published after subscription
5. **Async-safe publication** — handlers may call Send() or other Asynx APIs (no deadlock risk)

### For the Builder

1. **Immutability of returned instances** — Instance[T] methods must not mutate builder state
2. **Default values** — all optional WithX() methods have documented defaults
3. **Validation at build time only** — configuration errors are raised in Build(), not in individual With*() calls
4. **No state mutation** — calling Build() multiple times produces independent instances

---

## Example: Complete Configuration

```go
// Create and configure an Asynx instance for an Order aggregate
instance, err := asynx.New[Order]().
    WithEventStore(postgresEventStore).
    WithSnapshotStore(redisSnapshotStore).
    WithBus(kafkaBus).
    WithShardingOpts(asynx.ShardingOpts{
        Shards:     16,
        QueueDepth: 500,
    }).
    WithSchemaVersion(2).
    WithUpcaster(1, migrateOrderV1toV2).
    WithPanicHandler(func(e asynx.PanicEvent[Order]) {
        log.Printf("panic on %s: %v\n", e.EventName, e.Err)
    }).
    Build()

if err != nil {
    return fmt.Errorf("failed to build asynx: %w", err)
}

// instance is ready:
err = instance.Send(ctx, createOrderCmd)
err = instance.Subscribe("OrderPlaced", updateInventory)
orderState, err := instance.Get(ctx, orderID)
err = instance.Shutdown(shutdownCtx)
```

---

## Type Constraints

Asynx uses Go generics `[T any]` for the aggregate type. The aggregate type T:

- **Must be assignable by value** — T is passed and returned as values, not pointers
- **Should be immutable by convention** — aggregates should not be mutated in place
- **Must be JSON-serializable** — events are stored as JSON in the eventstore
- **Zero value must be valid** — T{} represents "aggregate does not exist yet"

Example valid aggregate:
```go
type Order struct {
    ID        string
    Status    string
    Items     []Item
    Total     float64
    CreatedAt time.Time
}

// Asynx instance for Order:
instance := asynx.New[Order]().
    WithEventStore(store).
    Build()

// Orders passed by value:
instance.Send(ctx, cmd)  // cmd.EmitEvent returns Order, not *Order
state, _ := instance.Get(ctx, orderID)  // state is Order, not *Order
```

---

## Error Types

Core does not define specific error types — it relies on standard `error` interface. Sub-packages (processor, eventstore, etc.) define their specific errors (e.g., `ErrValidation`, `ErrQueueFull`, `ErrNotFound`) and return them as `error` type.

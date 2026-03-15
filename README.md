# asynx

**Validate early, commit confidently.** Event sourcing + CQRS for Go with command-first validation and automatic event diffs.

## Install

```bash
go get github.com/char2cs/asynx
```

## Quick Start

**Define your aggregate:**
```go
type Order struct {
	ID     string
	Status string
	Total  float64
}
```

**Implement a command with validation:**
```go
type CreateOrderCmd struct {
	ID    string
	Total float64
}

func (c CreateOrderCmd) AggregateID() string { return c.ID }
func (c CreateOrderCmd) EventName() string   { return "OrderCreated" }
func (c CreateOrderCmd) ShouldSnapshot() bool { return false }

// Validation runs before any events are written
func (c CreateOrderCmd) Validate(current *Order) error {
	if c.Total <= 0 {
		return models.ErrValidation // Fails fast
	}
	if current != nil {
		return models.ErrValidation // Already exists
	}
	return nil
}

// Pure function: transform old state → new state
func (c CreateOrderCmd) EmitEvent(current *Order) Order {
	return Order{ID: c.ID, Status: "Pending", Total: c.Total}
}
```

**Initialize and send commands:**
```go
ax, _ := asynx.New[Order]().
	WithEventStore(memStore).
	Build()

err := ax.Send(ctx, CreateOrderCmd{ID: "ORD-001", Total: 99.99})
if err == models.ErrValidation {
	// Invalid command rejected before event creation
}

// Read current state (replayed from events)
order, _ := ax.Get(ctx, "ORD-001")
```

## Key Concepts

### Commands
Immutable mutation requests. Implement `Command[T]` with `Validate()` (fails fast) and `EmitEvent()` (pure).

```go
// Invalid command rejected before event creation
type UpdateOrderCmd struct {
	OrderID   string
	NewStatus string
}

func (c UpdateOrderCmd) Validate(current *Order) error {
	if current == nil {
		return models.ErrNotFound
	}
	if current.Status == "Shipped" {
		return models.ErrValidation // Can't update shipped orders
	}
	return nil
}

func (c UpdateOrderCmd) EmitEvent(current *Order) Order {
	order := *current
	order.Status = c.NewStatus
	return order
}
```

### Events
Automatic RFC 6902 JSON patches computed from state changes. Durably stored before publication.

```go
// Event contains both states
event.PreviousAggregate // Order{Status: "Pending"}
event.Aggregate         // Order{Status: "Confirmed"}
event.Version           // int64 (total changes for this aggregate)
event.EventName         // "OrderConfirmed"
event.OccurredAt        // timestamp
```

### Aggregates
Your domain model — any Go struct. Asynx replays events on read; you define initial state in `EmitEvent()` when current is nil.

```go
// Asynx replays all events and reconstructs state
order, err := ax.Get(ctx, "ORD-001")
// Returns latest state from all OrderCreated/OrderConfirmed/OrderShipped events
```

### Projections
Decoupled event subscribers. Can fail independently without affecting the event store.

```go
// Projection fails: event still safely stored
ax.Subscribe("Order.*", func(ctx context.Context, event models.Event[Order]) {
	// Can panic, timeout, or fail — events remain durable
	saveToDatabase(event) // Even if this fails
})
```

### Store
Persistent event backend. Bring your own (PostgreSQL, DynamoDB) or use in-memory for testing.

### Bus
Event publication after durably writing to store. In-process by default; can be replaced.

## Projections

Subscribe to events with regex patterns. Subscriptions are decoupled from command handling.

**Basic subscription:**
```go
// Match events by name (regex)
id, err := ax.Subscribe("Order.*", func(ctx context.Context, event models.Event[Order]) {
	// Called after event is durably stored
	fmt.Printf("%s: %s\n", event.EventName, event.Aggregate.ID)
})
```

**Specific event patterns:**
```go
// Only OrderShipped events
ax.Subscribe("OrderShipped", handler)

// OrderCreated or OrderConfirmed
ax.Subscribe("Order(Created|Confirmed)", handler)
```

**Fallback handler (failure resilience):**
```go
// If primary panics, fallback runs instead
ax.Subscribe("Order.*",
	primaryHandler,
	models.WithFallback[Order](fallbackHandler),
)
```

**Timeouts:**
```go
// Handler must complete within 5 seconds
ax.Subscribe("Order.*",
	handler,
	models.WithHandlerTimeout[Order](5 * time.Second),
)
```

**Access event details:**
```go
handler := func(ctx context.Context, event models.Event[Order]) {
	event.AggregateID         // "ORD-001"
	event.EventName           // "OrderConfirmed"
	event.Version             // Total changes for this aggregate
	event.Aggregate           // New state
	event.PreviousAggregate   // State before this event
	event.OccurredAt          // Timestamp
}
```

**Unsubscribe:**
```go
subID, _ := ax.Subscribe("Order.*", handler)
ax.Unsubscribe(subID) // Stop receiving events
```

## Schema Evolution

Handle breaking changes without data migration.

**Scenario: Adding a field to your aggregate:**
```go
// v1: type Order struct { ID, Total }
// v2: type Order struct { ID, Total, Status }
// Want: OrderCreated events from v1 get Status="Pending" automatically
```

**Register upcasters for version transitions:**
```go
ax := asynx.New[Order]().
	WithEventStore(store).
	WithSchemaVersion(2).                    // Current schema version
	WithUpcaster(1, upcastOrderV1toV2).      // v1→v2 transformation
	WithUpcaster(2, upcastOrderV2toV3).      // v2→v3 transformation
	Build()
```

**Implement an upcaster (transforms RFC 6902 patches):**
```go
func upcastOrderV1toV2(ctx context.Context, eventName string, patches []byte) ([]byte, error) {
	// patches is JSON RFC 6902 patch array
	// Add Status="Pending" to OrderCreated events
	if eventName == "OrderCreated" {
		// Parse, add Status op, return updated patches
		return addStatusPatch(patches)
	}
	return patches, nil
}
```

**How it works:**
- Old events stored with v1 patches → upcasted to v2 on replay
- New commands use v2 `EmitEvent()` → generate v2 patches
- Applied during both event reads and projection subscription
- No data migration needed

## Builder Configuration

| Method | Purpose | Default | Required |
|--------|---------|---------|----------|
| `WithEventStore(Store)` | Persistent event backend | — | **Yes** |
| `WithSnapshotStore(Store)` | Dedicated snapshot backend | Use event store | No |
| `WithBus(Bus[T])` | Custom event publisher | In-process channel bus | No |
| `WithShardingOpts(opts)` | Worker concurrency | 8 shards, unbounded queue | No |
| `WithSchemaVersion(int)` | Current aggregate schema | 1 | No |
| `WithUpcaster(from, fn)` | Schema version migration | — | No |
| `WithPanicHandler(fn)` | Projection panic handler | Log and continue | No |
| `WithCorruptionHook(fn)` | Snapshot corruption hook | Log and replay | No |

**Configure concurrency:**
```go
opts := asynx.ShardingOpts{
	Shards:     16,   // More goroutines for high throughput
	QueueDepth: 1000, // Bounded queue (0 = unbounded)
}

ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithShardingOpts(opts).
	Build()
```

**Handle projection panics:**
```go
ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithPanicHandler(func(ctx context.Context, event models.Event[Order], p any) {
		// Custom panic handling: alert, log, etc.
		log.Printf("Projection panicked on %s: %v", event.EventName, p)
	}).
	Build()
```

**Handle snapshot corruption:**
```go
ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithSnapshotStore(snapshotStore).
	WithCorruptionHook(func(err error) {
		// Called when snapshot can't deserialize
		// Falls back to replaying events
		metrics.SnapshotCorruptions.Inc()
	}).
	Build()
```

## Bring Your Own Store

Implement the `Store` interface for any persistent backend (PostgreSQL, DynamoDB, SQLite, etc.):

```go
type Store interface {
	// Append must enforce (aggregateID, version) uniqueness
	// This is the only synchronization needed for correctness
	Append(ctx context.Context, aggregateID string, version int64, data []byte) error

	// ReadFrom returns all events starting from version
	ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error)

	// ReadRange returns up to count events (for snapshots)
	ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error)

	// Count returns number of events since version
	Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error)
}
```

**Implement it for PostgreSQL:**
```go
type PostgresStore struct {
	db *sql.DB
}

func (s *PostgresStore) Append(ctx context.Context, aggID string, v int64, data []byte) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO events (aggregate_id, version, data) VALUES ($1, $2, $3)",
		aggID, v, data,
	)
	// Postgres UNIQUE(aggregate_id, version) enforces atomically
	return err
}

func (s *PostgresStore) ReadFrom(ctx context.Context, aggID string, fromV int64) ([][]byte, error) {
	rows, _ := s.db.QueryContext(ctx,
		"SELECT data FROM events WHERE aggregate_id=$1 AND version>=$2 ORDER BY version",
		aggID, fromV,
	)
	// ... scan and return patches
}
```

**Use your store:**
```go
pgStore := &PostgresStore{db: pgConn}

ax, _ := asynx.New[Order]().
	WithEventStore(pgStore).
	WithSnapshotStore(pgStore).  // Optional: separate snapshot backend
	Build()
```

**For testing, use in-memory store:**
```go
import "github.com/char2cs/asynx/internal/store"

ax, _ := asynx.New[Order]().
	WithEventStore(store.New()).  // Fast, no persistence
	Build()
```

## Error Handling

Common error types returned by Asynx:

| Error | When | Action |
|-------|------|--------|
| `ErrValidation` | `Validate()` returns an error | Command rejected; state unchanged |
| `ErrNotFound` | `Get()` or `Replay()` on missing aggregate | Check aggregate ID |
| `ErrQueueFull` | Send exceeds buffer (with `QueueDepth`) | Retry or increase capacity |
| `ErrShuttingDown` | Send after `Shutdown()` called | Wait for shutdown to complete |
| `ErrContextCancelled` | Context cancelled during operation | Check caller context |
| `ErrMissingEventStore` | `Build()` without `WithEventStore()` | Provide an event store |

See [github.com/char2cs/asynx/models/errors.go](./models/errors.go) for the full error list.

## Asynx Interface

```go
type Asynx[T any] interface {
	// Send executes a command
	Send(ctx context.Context, cmd models.Command[T]) error

	// Get reconstructs current aggregate state (replayed from events)
	Get(ctx context.Context, aggregateID string) (T, error)

	// Exists checks if an aggregate exists without full replay
	Exists(ctx context.Context, aggregateID string) (bool, error)

	// Preload caches an aggregate in memory for fast reads
	Preload(ctx context.Context, aggregateID string) error

	// Subscribe registers an event handler (async, non-blocking)
	Subscribe(pattern string, handler models.ProjectionHandler[T],
		opts ...models.SubscriptionOpt[T]) (string, error)

	// Unsubscribe stops an event handler
	Unsubscribe(id string) error

	// Replay runs a handler over a version range (for resync/migration)
	Replay(ctx context.Context, aggregateID string, fromVersion, toVersion int64,
		fn models.ProjectionHandler[T]) error

	// Shutdown gracefully stops command processing and event publication
	Shutdown(ctx context.Context) error

	// WaitPublish blocks until all async publishes complete (testing only)
	WaitPublish()
}
```

**Typical usage patterns:**

Send command and check for validation errors:
```go
err := ax.Send(ctx, cmd)
if err == models.ErrValidation {
	// Invalid command — event not created
}
```

Read current state:
```go
order, err := ax.Get(ctx, "ORD-001")
if err == models.ErrNotFound {
	// Aggregate doesn't exist
}
```

Preload for high-volume reads:
```go
ax.Preload(ctx, "ORD-001") // Cache in memory
order, _ := ax.Get(ctx, "ORD-001") // Fast: from cache
```

Replay events for reporting:
```go
ax.Replay(ctx, "ORD-001", 1, 10, func(ctx context.Context, event models.Event[Order]) {
	// Process events 1-10: can rebuild custom projections
})
```

Test synchronization:
```go
ax.Send(ctx, cmd1)
ax.Send(ctx, cmd2)
ax.WaitPublish() // Block until all events published to projections
// Now safe to check projection side effects
```

## Full Specification

For detailed design rationale, consistency guarantees, and advanced patterns, see the [docs/](./docs/spec).

## License

See [LICENSE](./LICENSE).

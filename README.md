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
import (
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store" // in-memory store for tests/examples
)

ax, _ := asynx.New[Order]().
	WithEventStore(store.New()).
	WithSnapshotStore(store.NewSnapshots()).
	Build()

event, err := ax.Send(ctx, CreateOrderCmd{ID: "ORD-001", Total: 99.99})
if errors.Is(err, models.ErrValidation) {
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
Automatic RFC 6902 JSON patches computed from state changes. Durably stored before publication. `Send` returns the resulting event once it is persisted.

```go
event, _ := ax.Send(ctx, ConfirmOrderCmd{OrderID: "ORD-001"})

event.PreviousAggregate // Order{Status: "Pending"}
event.Aggregate         // Order{Status: "Confirmed"}
event.Version           // int64 (total changes for this aggregate)
event.SchemaVersion     // Schema version the event was written with
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
Persistent event backend (`models.Store`). Bring your own (PostgreSQL, SQLite, DynamoDB) or use the bundled in-memory `store` package for testing. A separate `models.SnapshotStore` — one upserted row per aggregate, holding only the newest snapshot — is also required; see [Bring Your Own Store](#bring-your-own-store).

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

**Topic-style event names.** For dynamic, dotted event names (e.g. `EventName()` returns `"order.created." + id`), the `Topic` helper converts a `{{aggregate}}.{{action}}.{{id}}` pattern into an anchored regex — `*` in the middle matches one segment, a trailing `*` matches the rest:

```go
// Matches order.created.<any id>
ax.Subscribe(asynx.Topic("order.created.*"), handler)

// Matches any action on aggregate "order" for id ORD-001
ax.Subscribe(asynx.Topic("order.*.ORD-001"), handler)
```

`Listen` and `SubscribeWait` apply `Topic()` to their pattern automatically; `Subscribe` takes a raw regex, so wrap dotted patterns in `Topic()` yourself.

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
	event.SchemaVersion       // Schema version of the event
	event.Aggregate           // New state
	event.PreviousAggregate   // State before this event
	event.OccurredAt          // Timestamp
}
```

**Channel-based subscription:**
```go
// Receive exactly 3 matching events, then the channel closes automatically
ch, unsub, err := ax.Listen("order.created.*", 3)
defer unsub()
for event := range ch {
	fmt.Println(event.EventName)
}

// count <= 0: unbounded channel (capacity 16), never auto-closes.
// Call unsub() to clean up — and don't range after unsub().
```

**Wait for a single event:**
```go
// Block until the first matching event, or the context deadline
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
event, err := ax.SubscribeWait(ctx, "order.shipped.*")
```

**Unsubscribe:**
```go
subID, _ := ax.Subscribe("Order.*", handler)
ax.Unsubscribe(subID) // Stop receiving events
```

## Forgetting Aggregates

`Forget` permanently erases an aggregate — a tombstone event is written, all `ForgetHandler`s are notified with the last known state, then every event, snapshot, and cached entry is deleted. Useful for GDPR-style erasure.

```go
// React to erasure (e.g., clean up read models)
ax.OnForget(func(ctx context.Context, event models.Event[Order]) {
	deleteFromReadModel(event.AggregateID) // event.Aggregate = last known state
})

// Erase the aggregate
err := ax.Forget(ctx, "ORD-001")
if errors.Is(err, models.ErrValidation) {
	// Aggregate does not exist
}
```

Forget handlers can also be registered at build time with `WithForgetHandler`.

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
	WithSnapshotStore(snapshotStore).
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
| `WithSnapshotStore(SnapshotStore)` | Snapshot backend (one upserted row per aggregate) | — | **Yes** |
| `WithBus(Bus[T])` | Custom event publisher | In-process channel bus | No |
| `WithShardingOpts(opts)` | Worker concurrency | 8 shards, unbounded queue | No |
| `WithSchemaVersion(int)` | Current aggregate schema | 1 | No |
| `WithUpcaster(from, fn)` | Schema version migration | — | No |
| `WithPanicHandler(fn)` | Projection panic handler | Log and continue | No |
| `WithCorruptionHook(fn)` | Snapshot corruption hook | Log and replay | No |
| `WithPublishErrorHandler(fn)` | Observe async publish errors | Silently dropped | No |
| `WithForgetHandler(fn)` | Register forget handler at build time | — | No |

**Configure concurrency:**
```go
opts := asynx.ShardingOpts{
	Shards:     16,   // More goroutines for high throughput
	QueueDepth: 1000, // Bounded queue (0 = unbounded)
}

ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithSnapshotStore(snapshotStore).
	WithShardingOpts(opts).
	Build()
```

**Handle projection panics:**
```go
ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithSnapshotStore(snapshotStore).
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

**Observe async publish errors:**
```go
ax, _ := asynx.New[Order]().
	WithEventStore(store).
	WithSnapshotStore(snapshotStore).
	WithPublishErrorHandler(func(ctx context.Context, event models.Event[Order], err error) {
		// Event is already durably stored; this is observability only
		log.Printf("publish failed for %s: %v", event.EventName, err)
	}).
	Build()
```

## Bring Your Own Store

Two independent, required interfaces: `Store` for the event stream, and `SnapshotStore` for a single cached snapshot per aggregate. They are not interchangeable — `SnapshotStore` is not a second `Store` instance, and `WithSnapshotStore` does not default to the event store.

**`Store`** — implement for any persistent backend (PostgreSQL, SQLite, DynamoDB, etc.):
```go
type Store interface {
	// Append must enforce (aggregateID, version) uniqueness
	// This is the only synchronization needed for correctness
	Append(ctx context.Context, aggregateID string, version int64, data []byte) error

	// ReadFrom returns all events starting from version
	ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error)

	// ReadRange returns up to count events starting from version
	ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error)

	// Count returns number of events since version
	Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error)

	// Delete removes all records for the aggregate (used by Forget).
	// Must be idempotent — deleting a missing aggregate is not an error.
	Delete(ctx context.Context, aggregateID string) error
}
```

**`SnapshotStore`** — one upserted row per aggregate, never a versioned stream:
```go
type SnapshotStore interface {
	// Put replaces the snapshot for aggregateID. Implementations MUST upsert —
	// the primary key is aggregateID alone, never (aggregateID, version).
	Put(ctx context.Context, aggregateID string, version int64, data []byte) error

	// Get returns the stored snapshot. found == false means "never snapshotted"
	// (normal, not an error) — the caller falls back to replaying from event 1.
	Get(ctx context.Context, aggregateID string) (data []byte, found bool, err error)

	// Delete removes the snapshot for the aggregate (used by Forget).
	// Must be idempotent — deleting a missing aggregate is not an error.
	Delete(ctx context.Context, aggregateID string) error
}
```

**Implement `Store` for PostgreSQL:**
```go
type PostgresStore struct {
	db *sql.DB
}

func (s *PostgresStore) Append(ctx context.Context, aggID string, v int64, data []byte) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO events (aggregate_id, version, data) VALUES ($1, $2, $3)",
		aggID, v, data,
	)
	if isUniqueViolation(err) {
		// Postgres UNIQUE(aggregate_id, version) detects concurrent writers.
		// Map conflicts to ErrPipelineFailed so callers can retry (see Patterns).
		return fmt.Errorf("%w: version conflict", models.ErrPipelineFailed)
	}
	return err
}

func (s *PostgresStore) ReadFrom(ctx context.Context, aggID string, fromV int64) ([][]byte, error) {
	rows, _ := s.db.QueryContext(ctx,
		"SELECT data FROM events WHERE aggregate_id=$1 AND version>=$2 ORDER BY version",
		aggID, fromV,
	)
	// ... scan and return patches
}

func (s *PostgresStore) Delete(ctx context.Context, aggID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM events WHERE aggregate_id=$1", aggID)
	return err
}
```

**Implement `SnapshotStore` for PostgreSQL** — note the `ON CONFLICT` upsert, keyed on `aggregate_id` alone:
```go
type PostgresSnapshotStore struct {
	db *sql.DB
}

func (s *PostgresSnapshotStore) Put(ctx context.Context, aggID string, v int64, data []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshots (aggregate_id, version, data) VALUES ($1, $2, $3)
		ON CONFLICT (aggregate_id) DO UPDATE
			SET version = excluded.version, data = excluded.data
			WHERE excluded.version > snapshots.version`,
		aggID, v, data,
	)
	return err
}

func (s *PostgresSnapshotStore) Get(ctx context.Context, aggID string) ([]byte, bool, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT data FROM snapshots WHERE aggregate_id=$1", aggID,
	).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return data, err == nil, err
}

func (s *PostgresSnapshotStore) Delete(ctx context.Context, aggID string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM snapshots WHERE aggregate_id=$1", aggID)
	return err
}
```

**Use your stores:**
```go
eventStore := &PostgresStore{db: pgConn}
snapshotStore := &PostgresSnapshotStore{db: pgConn}

ax, _ := asynx.New[Order]().
	WithEventStore(eventStore).
	WithSnapshotStore(snapshotStore).  // Required — Build() errors without it
	Build()
```

**For testing, use the bundled in-memory stores:**
```go
import "github.com/char2cs/asynx/store"

ax, _ := asynx.New[Order]().
	WithEventStore(store.New()).            // Fast, no persistence
	WithSnapshotStore(store.NewSnapshots()). // Fast, no persistence
	Build()
```

Both in-memory stores also support one-shot failure injection for tests via `SetError(aggregateID, err)`.

See [docs/spec/store.md](./docs/spec/store.md) for the full `Store` and `SnapshotStore` contracts, including the SQL schema and error semantics.

## Breaking changes in v0.8.0

`WithSnapshotStore` now takes a `models.SnapshotStore`, not a `models.Store`, and is **required** — `Build()` returns `models.ErrMissingSnapshotStore` if it's not set. It no longer defaults to the event store.

Snapshots are a derived cache, so there is nothing to migrate: implement `models.SnapshotStore` against a table keyed by `aggregate_id` alone, drop the old snapshot table (or leave it orphaned), and pass the new store to `WithSnapshotStore`. The first `Get` per aggregate after the switch cold-replays its event stream once and writes a fresh snapshot row. Full details: [docs/spec/store.md § Migrating from v0.7.x](./docs/spec/store.md#migrating-from-v07x).

## Error Handling

All errors are sentinel values in the `models` package — match them with `errors.Is`:

| Error | When | Action |
|-------|------|--------|
| `ErrValidation` | `Validate()` rejects a command, or `Forget()` on a missing aggregate | Command rejected; state unchanged |
| `ErrNotFound` | `Get()` or `Replay()` on missing aggregate | Check aggregate ID |
| `ErrPipelineFailed` | Store `Append` failed (e.g. version conflict from a concurrent writer) | Safe to retry the command |
| `ErrQueueFull` | Send exceeds buffer (with `QueueDepth`) | Retry or increase capacity |
| `ErrShuttingDown` | Send after `Shutdown()` called | Wait for shutdown to complete |
| `ErrAlreadyShuttingDown` | `Shutdown()` called twice | Await the first call |
| `ErrContextCancelled` | Context cancelled during operation | Check caller context |
| `ErrMissingEventStore` | `Build()` without `WithEventStore()` | Provide an event store |
| `ErrMissingSnapshotStore` | `Build()` without `WithSnapshotStore()` | Provide a snapshot store |
| `ErrForgetFailed` | `Forget()` tombstoned the aggregate but deletion failed | Retry `Forget` |
| `ErrEmptyPattern` | `Listen()`/`SubscribeWait()` with empty pattern | Provide a pattern |
| `ErrNilHandler` | `Subscribe()` with nil handler | Provide a handler |
| `ErrBusClosed` | Subscribe/publish after bus shutdown | Rebuild or stop using the instance |
| `ErrDispatcherClosed` | Internal dispatch after shutdown | Stop sending |

See [models/errors.go](./models/errors.go) for the full list.

## Asynx Interface

```go
type Asynx[T any] interface {
	// Send validates and persists the command, returning the resulting event
	// once durably written. Projections fire asynchronously.
	Send(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)

	// SendWait is Send, but also blocks until all matching projections finish
	SendWait(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)

	// Get reconstructs current aggregate state (replayed from events)
	Get(ctx context.Context, aggregateID string) (T, error)

	// Exists checks if an aggregate exists without full replay
	Exists(ctx context.Context, aggregateID string) (bool, error)

	// Preload caches an aggregate in memory for fast reads
	Preload(ctx context.Context, aggregateID string) error

	// Forget tombstones the aggregate, notifies ForgetHandlers, then erases
	// all events, snapshots, and cached state
	Forget(ctx context.Context, aggregateID string) error

	// OnForget registers a handler invoked when any aggregate is forgotten
	OnForget(fn models.ForgetHandler[T]) (string, error)

	// Subscribe registers an event handler (async, non-blocking)
	Subscribe(pattern string, handler models.ProjectionHandler[T],
		opts ...models.SubscriptionOpt[T]) (string, error)

	// Unsubscribe stops an event handler
	Unsubscribe(id string) error

	// Listen opens a channel-based subscription; count > 0 auto-closes
	// after count events, count <= 0 is unbounded (call unsub to clean up)
	Listen(pattern string, count int) (<-chan models.Event[T], func(), error)

	// SubscribeWait blocks until the first matching event or ctx is done
	SubscribeWait(ctx context.Context, pattern string) (models.Event[T], error)

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
_, err := ax.Send(ctx, cmd)
if errors.Is(err, models.ErrValidation) {
	// Invalid command — event not created
}
```

Send and wait when the caller needs projections to be consistent:
```go
event, err := ax.SendWait(ctx, cmd)
// All subscribed projections have completed for this event
```

Read current state:
```go
order, err := ax.Get(ctx, "ORD-001")
if errors.Is(err, models.ErrNotFound) {
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

## Production Patterns

Battle-tested patterns for running asynx in real applications — optimistic-concurrency retries, graceful shutdown with background reactions, crash recovery via replay, and resilient projections — are collected in [docs/spec/patterns.md](./docs/spec/patterns.md).

## Full Specification

For detailed design rationale, consistency guarantees, and advanced patterns, see the [docs/](./docs/spec).

## License

See [LICENSE](./LICENSE).

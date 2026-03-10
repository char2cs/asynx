# EventStore Package Specification

## Overview

The `eventstore` package is the single persistence boundary in Asynx. Everything that touches durable storage — reading, writing, caching, hydrating, and schema migration — lives here. The processor calls into eventstore to get current state and to commit events. It never touches the Store directly.

Internally, eventstore is composed of three sub-responsibilities:

1. **reader** — Serves the latest committed state of an aggregate (warm/cold path hydration, StateView API)
2. **writer** — Boundary where commands become events (diffing, RFC 6902 serialization, append, snapshot flag)
3. **replayer** — Version-ordered event iteration with schema upcasting and automatic migration snapshots

The eventstore owns stream naming, event serialization, snapshot logic, and version management. The developer's Store owns durability, consistency, and multi-node coordination.

---

## Sub-Module: Reader

### Purpose

Serves the latest committed state of an aggregate to the processor. The path taken (snapshot-based or full replay) is entirely determined by what the stores return — no external signal or cache layer is involved.

### Public API

```go
// Get(ctx, aggregateID) returns the current aggregate state
// Returns error if aggregate has never existed
Get(ctx context.Context, aggregateID string) (T, error)

// Exists(ctx, aggregateID) checks if aggregate exists without loading full state
Exists(ctx context.Context, aggregateID string) (bool, error)

// Preload(ctx, aggregateID) eagerly hydrates aggregate state for later access
// Used to pay the cold path cost at startup instead of on first Send()
Preload(ctx context.Context, aggregateID string) error

// StateView API (available as asynx.Get, asynx.Exists, asynx.Preload)
```

### Warm Path Hydration

When a snapshot exists for the aggregate:

```
1. ReadFrom(snapshotStore, aggregateID, 0)
   ↓ snapshot found at version N
2. ReadFrom(eventStore, aggregateID, N+1)
   ↓ some events exist after snapshot
3. Replay delta events (version N+1 through latest) on top of snapshot state
4. Return rehydrated state
```

**Cost:** Snapshot load + delta replay. Fast for frequently-updated aggregates.

**Invariant:** Snapshot version and event stream versions are always in sync. If snapshot is at version 5, the next event is at version 6. No gaps.

### Cold Path Hydration

When no snapshot exists:

```
1. ReadFrom(snapshotStore, aggregateID, 0)
   ↓ no snapshot
2. ReadFrom(eventStore, aggregateID, 0)
   ↓ events exist (or don't)
3. If no events: ErrNotFound
   If events exist: Replay all events from scratch
4. Return rehydrated state
```

**Cost:** Full stream replay. Slow for aggregates with long histories.

**Prevention:** Use `ShouldSnapshot() bool` on commands to mark important checkpoints. After first snapshot, subsequent loads use warm path.

**Optimization:** Call `Preload(ctx, aggregateID)` at startup to pay cold path cost before live traffic.

### Method: `Get`

**Signature**
```go
Get(ctx context.Context, aggregateID string) T, error
```

**Purpose**
Returns the latest committed state of an aggregate. Implements cold and warm paths transparently.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to load (non-empty string)

**Return Values**
- `T` — current aggregate state (value, not pointer)
- `error` — non-nil if:
  - Aggregate has never existed (`ErrNotFound`)
  - Store read failed (context cancelled, storage error, etc.)

**Invariants**
- **Consistency is strong** — reads the latest committed state
- **Path is transparent** — caller never knows if warm or cold path was used
- **Zero value means does not exist** — T{} is returned only if aggregate doesn't exist and ErrNotFound is returned

**Side Effects**
- Reads from snapshot store (may cache internally)
- Reads from event store (may cache internally)
- No writes to either store

**Error Handling**
- Aggregate not found → `ErrNotFound`
- Context cancelled → context error
- Storage error → propagates

**Example**
```go
// Get current order state:
order, err := instance.Get(ctx, "order_123")
if err == asynx.ErrNotFound {
    // Order doesn't exist
    return
}
if err != nil {
    // Storage or context error
    log.Fatal(err)
}

// order is fully hydrated and ready to use
fmt.Println(order.Status)  // "paid", "shipped", etc.
```

---

### Method: `Exists`

**Signature**
```go
Exists(ctx context.Context, aggregateID string) (bool, error)
```

**Purpose**
Checks if an aggregate exists without loading full state. More efficient than Get() when you only need existence check.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to check (non-empty string)

**Return Values**
- `bool` — true if aggregate has at least one event, false if never existed
- `error` — non-nil if check failed (storage error, context cancelled)

**Invariants**
- **Efficient for non-existence checks** — doesn't load full state
- **No side effects** — pure read

**Implementation Detail**
Typically checks if snapshot or event stream exists, without loading all data.

**Error Handling**
- Storage error → propagates
- Context cancelled → context error

**Example**
```go
// Check if order exists before attempting to ship:
exists, err := instance.Exists(ctx, "order_999")
if err != nil {
    log.Fatal(err)
}

if !exists {
    return fmt.Errorf("order does not exist")
}

// Safe to proceed with command:
err := instance.Send(ctx, shipOrderCmd)
```

---

### Method: `Preload`

**Signature**
```go
Preload(ctx context.Context, aggregateID string) error
```

**Purpose**
Eagerly hydrates aggregate state and caches it. Used to pay the cold path cost at startup, before live traffic arrives.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to preload (non-empty string)

**Return Values**
- `error` — non-nil if preload failed (storage error, context cancelled)

**Invariants**
- **Idempotent** — calling multiple times is safe (at most preloads once)
- **No guarantees after preload** — cache may be evicted; subsequent Get() may still need to reload

**Use Case**
High-concurrency aggregates with long histories. Without preload:

```
User action arrives
  ↓
First Send(ctx, cmd) hits cold path
  ↓
Replay all 10000 events to hydrate
  ↓
User waits (potentially seconds)
```

With preload at startup:

```
Application starts
  ↓
Preload(ctx, "hot_aggregate_123")
  ↓ (replay happens now, offline)
Cost paid before any users arrive
  ↓
User action arrives
  ↓
Send(ctx, cmd) uses warm or cached path
  ↓
Instant response
```

**Example**
```go
// At startup, preload hot aggregates:
err := instance.Preload(ctx, "order_123")  // Expected to have long history
if err != nil {
    log.Printf("preload failed: %v\n", err)
    // Not fatal — just means next access will be slow
}

// Later, same aggregate is fast:
err := instance.Send(ctx, shipCmd)  // Uses warm path, fast
```

---

### StateView API Summary

Typically exposed as top-level functions on the Instance:

```go
instance := asynx.New[Order]().Build()

// Get current state
state, err := instance.Get(ctx, aggregateID)

// Check existence
exists, err := instance.Exists(ctx, aggregateID)

// Preload for performance
err := instance.Preload(ctx, aggregateID)
```

These are **strongly consistent** reads. Use for:
- Loading state before issuing commands
- Checking preconditions
- Building single-aggregate reads in REST APIs

For **eventually consistent** cross-aggregate views, use projections (subscription callbacks that maintain read models).

---

## Sub-Module: Writer

### Purpose

The boundary where commands become events. Responsible for:
1. Diffing old and new state
2. Serializing to RFC 6902 patches
3. Appending to event stream
4. Optionally writing snapshots
5. Wrapping in Event envelope

### Public API

```go
// Write is called by processor after command validation and state emission
Write(ctx context.Context, aggregateID string, eventName string, oldState T, newState T, version int64, shouldSnapshot bool) (Event[T], error)
```

### Method: `Write`

**Signature**
```go
Write(ctx context.Context, aggregateID string, eventName string, oldState T, newState T, version int64, shouldSnapshot bool) (Event[T], error)
```

**Purpose**
Commits a command's state transition to the eventstore and optionally to the snapshot store.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — aggregate being updated (non-empty)
- `eventName` — name of the event (from `cmd.EventName()`, non-empty)
- `oldState` — state before the command (may be zero value if first event)
- `newState` — state after the command (never pointer, never zero value)
- `version` — version number for this event (monotonically increasing per aggregate)
- `shouldSnapshot` — flag from `cmd.ShouldSnapshot()` to trigger snapshot storage

**Return Values**
- `Event[T]` — the fully-populated event that was written
  - ID, AggregateID, EventName, Version, SchemaVersion, OccurredAt all set
  - Aggregate = newState
  - PreviousAggregate = oldState
- `error` — non-nil if write failed

**Internal Process**

1. Serialize oldState to JSON
2. Serialize newState to JSON
3. Compute RFC 6902 diff (JSON Patch) comparing old → new
4. Create Event[T] envelope with:
   - ID: unique event ID (UUID or similar)
   - AggregateID: from parameter
   - EventName: from parameter
   - Version: from parameter
   - SchemaVersion: stamped from builder's WithSchemaVersion()
   - OccurredAt: current time
   - Aggregate: newState
   - PreviousAggregate: oldState
5. Serialize Event[T] to JSON
6. Append to event stream (events:aggregateID) via Store.Append()
   - **Save point** ← event is now durable
7. If shouldSnapshot, append to snapshot stream (snapshots:aggregateID):
   - Serialize full newState to JSON
   - Include version and schemaVersion in snapshot metadata
   - Append via Store.Append()

**Invariants**
- **Event is durable before returning (save point)** — Event Append must succeed; Write() is idempotent only at the event stream level
- **Snapshot write is synchronous** — If `shouldSnapshot=true`, snapshot write is part of the Write() call; snapshot failure returns error
- **RFC 6902 is the stable format** — all events are stored as JSON patches, never as full state (except snapshots)
- **Diff is never exposed to developer** — Event[T] contains oldState and newState, not the patch

**Side Effects**
- Writes to event stream (append only)
- May write to snapshot stream if shouldSnapshot == true
- Updates version tracking (version counter incremented by caller before Write)

**Error Handling**

1. **Event stream write failure** — Uniqueness violation on Append
   - (aggregateID, version) already exists
   - Return error
   - Processor interprets as ErrPipelineFailed
   - Caller retries Send() from scratch
   - **Event was NOT written** (save point not reached)

2. **Snapshot stream write failure** — Append fails when shouldSnapshot=true
   - Snapshot write is part of Write() call
   - If snapshot Append fails, return error
   - **Event WAS written to event stream** (save point already passed)
   - **But snapshot write failed** — event is durable, but rehydration will be slower next time
   - Processor interprets as ErrPipelineFailed
   - Caller retries Send() from scratch
   - **Important:** On retry, event at this version already exists; write fails on uniqueness, caller reloads and continues

3. **Context cancelled or deadline exceeded**
   - Return context error
   - Caller handles (user cancelled request, timeout, etc.)

4. **Storage unavailable** — disk full, connection lost, permission denied
   - Return error
   - Caller retries or fails
   - For event stream: event may or may not have been written (depends on when failure occurred)
   - For snapshot stream: less critical (snapshot is optional)

5. **JSON serialization error** — aggregate is not JSON-serializable
   - This is a programming error (aggregate struct is malformed)
   - Return error
   - Caller cannot recover (aggregate type is broken)

**Example**
```go
// Processor calls Write after successful validation:
oldState := loadedOrder          // Current state (may be zero value)
newState := order                // New state (from cmd.EmitEvent())
version := int64(5)              // Next version number
shouldSnapshot := cmd.ShouldSnapshot()  // Command decides

event, err := writer.Write(ctx, "order_123", "OrderShipped", oldState, newState, version, shouldSnapshot)
if err != nil {
    if isSomeKindOfVersionConflict(err) {
        return ErrPipelineFailed  // Caller will retry
    }
    return err  // Other error (context, storage)
}

// event is now:
// {
//   ID: "evt_xyz",
//   AggregateID: "order_123",
//   EventName: "OrderShipped",
//   Version: 5,
//   SchemaVersion: 1,
//   OccurredAt: 2024-01-15T10:30:00Z,
//   Aggregate: newState,
//   PreviousAggregate: oldState
// }

// It's durable in eventstore. Safe to publish to bus:
bus.Publish(ctx, event)
```

---

### RFC 6902 Serialization

Events are stored as RFC 6902 JSON Patches. Example:

```json
[
  { "op": "replace", "path": "/status", "value": "shipped" },
  { "op": "replace", "path": "/shipDate", "value": "2024-01-15T10:30:00Z" },
  { "op": "add", "path": "/trackingNumber", "value": "TRK123456" }
]
```

The patch represents the minimum diff between oldState and newState. RFC 6902 is:
- **Standard** — defined by RFC 6902, language-agnostic
- **Human-readable** — easy to inspect in databases
- **Tooled** — many libraries support it
- **Stable** — the permanent storage format for all events

Asynx computes the diff automatically; developers never see it. It's internal to the eventstore.

---

## Sub-Module: Replayer

### Purpose

Version-ordered event iteration with schema migration support. Used by:
1. Reader (warm and cold path rehydration)
2. Public Replay API (recovery and manual re-projection)
3. Schema migration (upcasting, automatic snapshots)

### Public API

```go
// Replay is exposed as asynx.Replay()
Replay(ctx context.Context, aggregateID string, fromVersion int64, toVersion int64, fn func(Event[T])) error

// Called by reader during rehydration
ReplayInto(ctx context.Context, aggregateID string, fromVersion int64, state *T) (T, error)
```

### Method: `Replay` (Public API)

**Signature**
```go
Replay(ctx context.Context, aggregateID string, fromVersion int64, toVersion int64, fn func(Event[T])) error
```

**Purpose**
Iterates events from the eventstore in strict version order, applying upcasters as needed, and calls `fn` for each event. Used for projection recovery after failures.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — aggregate to replay (non-empty)
- `fromVersion` — inclusive starting version (>= 0)
  - `0` means start from version 1
- `toVersion` — inclusive ending version
  - `0` means replay through to the latest event
- `fn` — callback function called for each event in order

**Return Values**
- `error` — non-nil if replay failed (aggregate not found, context cancelled, etc.)

**Invariants**
- **Version order is strict** — events arrive in ascending version order, no gaps
- **No snapshots created** — Replay is read-only, never triggers snapshot writes
- **Upcasters are applied** — old events are migrated to current schema version before callback
- **Callback sees migrated events** — fn receives Event[T] with current schema applied
- **All or nothing** — either all events are processed, or none are (no partial replay)

**Side Effects**
- None — pure read operation
- (Schema version changes can trigger snapshots after replay — see upcasting section)

**Error Handling**

1. **Aggregate not found** — no events for aggregateID
   - Return ErrNotFound
   - fn is never called

2. **Version range invalid** — fromVersion > toVersion (and toVersion != 0)
   - Return error
   - fn is never called

3. **Context cancelled or deadline exceeded**
   - Return context error
   - fn may be partially called (for events already read)

4. **Storage error** — read failed
   - Return error
   - fn may be partially called

5. **Upcaster error** — upcaster function panics or returns invalid data
   - Return error
   - Callback not called for this event or beyond

**Example: Projection Recovery**
```go
// Projection callback failed mid-execution. Replay to re-run it:
err := instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    // Re-run projection logic:
    // - Update read model
    // - Send side effects
    // - Etc.
    readModel.UpdateOrder(e.Aggregate)
})

if err != nil {
    log.Printf("replay failed: %v\n", err)
}
// If successful, read model is now consistent
```

**Example: Partial Replay**
```go
// Replay only version 5 through 10:
err := instance.Replay(ctx, "order_123", 5, 10, func(e asynx.Event[Order]) {
    log.Printf("version %d: %s\n", e.Version, e.EventName)
})
// Calls fn 6 times (versions 5, 6, 7, 8, 9, 10)
```

---

### Method: `ReplayInto` (Internal)

**Signature**
```go
ReplayInto(ctx context.Context, aggregateID string, fromVersion int64, state *T) (T, error)
```

**Purpose**
Internal method used by reader to rehydrate state by replaying events. Applies events to an initial state and returns the final rehydrated state.

**Parameters**
- `ctx` — context for cancellation
- `aggregateID` — aggregate to replay
- `fromVersion` — version to start replaying from
- `state` — pointer to initial state (may point to zero value or snapshot state)

**Return Values**
- `T` — final rehydrated state
- `error` — replay error

**Used By**
- Cold path: ReplayInto from version 0 with zero-value state
- Warm path: ReplayInto from version N+1 with snapshot state

---

### Schema Upcasting

When an event's SchemaVersion is lower than the current instance's SchemaVersion, the replayer runs the upcaster chain before applying the patch.

**Example: Schema Migration**

```
Instance created with WithSchemaVersion(3) and two upcasters registered:
  WithUpcaster(1, v1to2Upcaster)
  WithUpcaster(2, v2to3Upcaster)

Event stored at SchemaVersion 1 is loaded:
  ↓
1. Read raw RFC 6902 patch from stream
2. Check SchemaVersion: 1 < 3 (current)
3. Run upcaster(1) on raw bytes → produces v2-compatible bytes
4. Run upcaster(2) on v2 bytes → produces v3-compatible bytes
5. Apply corrected patch to aggregate state
```

**Upcaster Signature**
```go
func(eventName string, raw []byte) []byte
```

**Parameters**
- `eventName` — name of the event (e.g., "OrderPlaced")
- `raw` — RFC 6902 patch bytes to fix

**Return**
- `[]byte` — corrected patch bytes for the next version

**Example Upcaster**
```go
WithUpcaster(1, func(eventName string, raw []byte) []byte {
    // Fix: rename "/status" → "/state" in the JSON patch
    return bytes.ReplaceAll(raw,
        []byte(`"/status"`),
        []byte(`"/state"`))
})
```

**Automatic Post-Upcast Snapshot**

After an event is fully upcasted and applied to rehydrate state, the replayer signals the writer to create a snapshot at the current schema version:

```
Read old event at SchemaVersion 1
  ↓
Upcast to SchemaVersion 3
  ↓
Apply patch to state
  ↓
Writer.Write() called with shouldSnapshot=true (auto-triggered)
  ↓
Snapshot stored at current version
```

This "seals the migration" — subsequent loads skip the upcaster chain entirely by loading the snapshot directly. **The migration cost is paid once per aggregate, on first access after schema version bump.**

**Hard Prohibition: Replay Never Triggers Snapshots**

Public `Replay()` API never creates snapshots — it's read-only:

```go
// Public Replay NEVER triggers snapshots, even if upcasting happens:
instance.Replay(ctx, "order_123", 0, 0, fn)
// Even if upcasters run and old events are migrated,
// NO snapshot is written
```

Manual recovery via Replay should not pollute the snapshot stream. Only automatic rehydration during Get() can trigger upcaster snapshots.

---

## Interaction Between Sub-Modules

### Command Execution Flow

```
Processor.Send(cmd)
  ↓
→ reader.Get(aggregateID)
    Warm or cold path rehydration
    Uses replayer internally to apply patches
  ↓
  Current state returned
  ↓
→ cmd.Validate(currentState)
  cmd.EmitEvent(currentState)
  ↓
→ writer.Write(aggregateID, eventName, old, new, version, snapshot)
    Create Event envelope
    Compute RFC 6902 diff
    Append to eventstore
    Optionally write snapshot
  ↓
  Event returned, event is durable
  ↓
→ bus.Publish(event)
    Calls subscription handlers
```

### Snapshot Out of Sync Recovery

If snapshot version doesn't match event stream version (e.g., partial write crash):

```
Snapshot exists at version 5
Event stream has versions 1-7
Reader detects version mismatch
  ↓
Uses replayer to fetch events from version 6-7
  ↓
Applies patches on top of snapshot
  ↓
Detects event #6 is already in snapshot? Skips it.
  ↓
Returns rehydrated state
```

This is automatic and transparent to the processor.

---

## Configuration via Builder

Schema version and upcasters are set on the builder:

```go
asynx.New[Order]().
    WithEventStore(store).
    WithSchemaVersion(2).
    WithUpcaster(1, migrateV1toV2).
    Build()
```

Once built, eventstore uses:
- Current schema version: 2
- Upcasters: registered chain for 1→2

Changes to schema require a new instance.

---

## Error Types

Eventstore returns standard `error` interface. Specific errors:

- `ErrNotFound` — aggregate has never existed
- `ErrPipelineFailed` — write failed (typically version conflict)
- Context errors — cancellation, deadline
- Storage errors — propagated from Store

---

## Memory and Performance Considerations

### Cold Path Cost

Aggregates with long histories pay the cost on first access:

```
Load 10000-event aggregate
  ↓
Read all 10000 events from storage
  ↓
Replay each event against state
  ↓
Return final state (seconds may pass)
```

**Mitigation:**
- Call `Preload(ctx, aggregateID)` at startup for hot aggregates
- Use `ShouldSnapshot()` to checkpoint important versions
- Cache snapshots in high-traffic stores (Redis)

### Snapshot Storage

Snapshots consume additional storage but speed up rehydration. Decisions:

- **No snapshots** — cold path always (slowest)
- **Some snapshots** — mixed warm and cold (balanced)
- **All snapshots** — warm path always (fastest, most storage)

Use `ShouldSnapshot()` on commands to mark important checkpoints. The framework respects the command's decision.

### Upcasting Overhead

Upcasters run on every old event during replay. Cost is O(number of events):

```
Aggregate has 1000 events at SchemaVersion 1
Instance is at SchemaVersion 3
  ↓
Load aggregate
  ↓
Replayer reads all 1000 events
  ↓
Runs upcaster(1→2) on each
  ↓
Runs upcaster(2→3) on each
  ↓
Applies patch
  ↓
Total cost: high (but happens once, snapshot seals it)
```

After upcasting, snapshot is written. Next access loads snapshot directly (warm path) and skips upcasters.

---

## Example: Complete EventStore Usage

```go
// Build instance with eventstore config:
instance, _ := asynx.New[Order]().
    WithEventStore(postgresStore).
    WithSnapshotStore(redisStore).  // Optional: fast snapshots
    WithSchemaVersion(2).
    WithUpcaster(1, func(name string, raw []byte) []byte {
        // Fix schema v1 → v2: rename "/status" to "/state"
        return bytes.ReplaceAll(raw, []byte(`"/status"`), []byte(`"/state"`))
    }).
    Build()

// StateView API (strongly consistent reads):
order, err := instance.Get(ctx, "order_123")
// Uses warm/cold path, loads current state

exists, err := instance.Exists(ctx, "order_456")
// Check existence without loading full state

err := instance.Preload(ctx, "hot_order_789")
// Pay cold path cost now, before live traffic

// Send command (eventstore writes event and snapshot):
err := instance.Send(ctx, shipCmd)
// Triggers reader.Get → writer.Write → bus.Publish

// Replay for recovery (read-only, read-only):
err := instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    readModel.UpdateOrder(e)
})
// Iterates all events, applies upcasters if needed
// Does NOT trigger snapshots
```

---

## Known Limitations

**No consistent snapshots across multiple aggregates.** Each aggregate's snapshot is taken independently. Cross-aggregate consistency is eventual only.

**Upcaster chain must be idempotent.** If an upcaster runs twice on the same bytes (due to retry), it should produce the same output. Design upcasters accordingly.

**No schema downgrades.** Asynx only upcasts (forward migrations). Downgrades must be handled manually (create a new schema version that represents the downgrade).

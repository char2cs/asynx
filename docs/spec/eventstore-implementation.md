# EventStore Implementation Specification

## Overview

This specification details the **internal architecture and mechanics** of the eventstore module. It explains how state is loaded, how events are written, how schema migrations work, and how the three sub-modules (reader, writer, replayer) interact.

The eventstore is the single persistence boundary in Asynx. Everything durable goes through here: reading state, writing events, computing diffs, managing snapshots, and handling schema migrations via upcasters.

**Key Design Principles:**
- **Optimistic snapshots** — assume valid, recover only on error
- **Full-state diffing** — RFC 6902 standard, no field-level reflection
- **Always-upcast Replayer** — callbacks see consistent schema
- **No in-memory caching** — storage is source of truth, operators layer Redis/Memcached
- **Fail-fast upcasters** — schema migrations are developer responsibility

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────┐
│               EventStore[T]                          │
│  (reader, writer, replayer, store, builder config)  │
└────────────────┬─────────────────────────────────────┘
                 │
        ┌────────┼────────┐
        │        │        │
    ┌───▼──┐ ┌──▼───┐ ┌──▼────────┐
    │Reader│ │Writer│ │ Replayer  │
    │      │ │      │ │           │
    │ • Get│ │ • Diff│ │ • Iterate │
    │ • Ex │ │ • Snap│ │ • Upcast  │
    │ • Pr │ │ • Appe│ │ • Replay  │
    └───┬──┘ └──┬───┘ └──┬────────┘
        │       │        │
        └───────┼────────┘
                │
         ┌──────▼──────┐
         │   Store     │
         │  (Append)   │
         │ (ReadFrom)  │
         │(ReadRange)  │
         └─────────────┘
```

---

## Core Types

### Internal Event Type (EventStore-Only, Not Public)

```go
// internalEvent is the representation stored in the event stream
// It contains patches (not full state) plus metadata needed for reconstruction
type internalEvent struct {
    ID            string                               // Unique event ID
    EventName     string                               // From command.EventName()
    Version       int64                                // Monotonic version for aggregate
    SchemaVersion int                                  // Schema version at write time
    OccurredAt    time.Time                            // When event occurred
    Patches       []jsonpatch.JsonPatchOperation       // RFC 6902 patches (old→new state)
}

// Serialized to JSON and stored as opaque bytes in Store:
// {"ID": "...", "EventName": "...", "Version": 1, "SchemaVersion": 2, "OccurredAt": "...", "Patches": [...]}
```

**Why separate from public Event[T]:**
- Public Event[T] has Aggregate + PreviousAggregate (full state) for projections
- internalEvent has Patches only (efficient storage)
- Single source of truth: patches in storage, full state reconstructed on load

---

### Snapshot Blob Structure (Internal)

```go
// snapshotBlob is stored in the snapshot stream
// It includes version info so Reader knows where to resume event loading
type snapshotBlob struct {
    Version       int64  // Version this snapshot was taken at
    SchemaVersion int    // Schema version at snapshot time
    State         []byte // Serialized aggregate state (T)
}

// Serialized to JSON and stored as opaque bytes in Store:
// {"Version": 5, "SchemaVersion": 2, "State": {...full serialized state...}}
```

**Why version in snapshot:**
- Reader needs to know snapshot is at version 5
- Then loads events starting at version 6
- No guessing or side-channel lookups needed

---

### Reader Sub-Module

```go
type Reader[T any] struct {
    store                  Store          // event stream
    snapshotStore          SnapshotStore  // separate interface — not a second Store
    stateDeserializer      func([]byte) (T, error)        // Deserialize T from bytes
    replayer               *Replayer[T]                   // For hydration
    stateZeroValue         T
}

// Public API
func (r *Reader[T]) Get(ctx context.Context, aggregateID string) (T, error)
func (r *Reader[T]) Exists(ctx context.Context, aggregateID string) (bool, error)
func (r *Reader[T]) Preload(ctx context.Context, aggregateID string) error
```

**Invariants:**
- No in-memory cache — every Get() queries storage
- Strong consistency — always returns latest committed state
- Zero value means aggregate doesn't exist (ErrNotFound)
- Uses Replayer for hydration (applies patches + upcasting)

---

### Writer Sub-Module

```go
type Writer[T any] struct {
    store                  Store          // event stream
    snapshotStore          SnapshotStore  // separate interface — not a second Store
    stateSerializer        func(T) ([]byte, error)        // Serialize T to bytes
    diffComputer           *RFC6902Computer               // Compute patches
    currentSchemaVersion   int
}

// Public API
func (w *Writer[T]) Write(
    ctx context.Context,
    aggregateID string,
    eventName string,
    previousState T,
    newState T,
    version int64,
    shouldSnapshot bool,
) (Event[T], error)
```

**Invariants:**
- Computes RFC 6902 patches (full old→new state diff)
- Serializes patches + metadata into internalEvent
- Appends internalEvent to Store (SAVE POINT)
- Snapshot upserted via SnapshotStore.Put only if shouldSnapshot=true
- Returns public Event[T] for bus publish

---

### Replayer Sub-Module

```go
type Replayer[T any] struct {
    snapshotStore          SnapshotStore                  // For writing auto-snapshots (Put, not Append)
    stateDeserializer      func([]byte) (T, error)
    stateSerializer        func(T) ([]byte, error)        // For snapshot serialization
    patchApplier           *RFC6902Applier                // Applies RFC 6902 patches
    upcasters              map[int]UpcasterFunc[T]        // SchemaVersion → func (1→2, 2→3, etc)
    currentSchemaVersion   int
    stateZeroValue         T
}

// Upcaster transforms internalEvent from one schema version to the next
type UpcasterFunc[T any] func(internalEvent) (internalEvent, error)
// Example: Upcaster[1] transforms SchemaVersion 1 → 2
//          Upcaster[2] transforms SchemaVersion 2 → 3

// Public API
func (r *Replayer[T]) Replay(
    ctx context.Context,
    aggregateID string,
    fromVersion int64,
    toVersion int64,
    fn func(Event[T]),
) error

// Internal (used by Reader)
func (r *Replayer[T]) hydrate(
    ctx context.Context,
    aggregateID string,
    seedState T,
    internalEvents []internalEvent,
) (T, error)
```

**Invariants:**
- Hydrate applies patches + upcasts + auto-snapshots (self-contained)
- Always upcasts events to currentSchemaVersion
- Upcasters are chainable (1→2→3 → currentSchema)
- Panics in upcasters propagate to caller (fail fast)
- Replay() is read-only (yields upcasted public Event[T], never writes snapshots)
- Hydrate() writes auto-snapshots when upcasting occurs (seals migration)

---

## Sub-Module: Reader

### Purpose

Loads the current aggregate state using the optimal path: snapshot+delta if available, full replay if not. Transparent to caller — they never know which path was taken.

### Warm Path: Snapshot + Delta Replay

```
Scenario: Snapshot exists at version 5, events exist at 6-7

1. SnapshotStore.Get(ctx, aggregateID)
   ↓
2. Success: snapshot blob at version 5 found
   Deserialize → aggregate state at version 5

3. Store.ReadFrom(ctx, "events:{aggregateID}", 6)
   ↓
4. Success: event blobs at versions 6, 7

5. Replayer.hydrate(snapshotState, [eventBlob6, eventBlob7])
   ↓
6. Apply RFC 6902 patches, upcast each event to currentSchema
   event6: deserialize patches → apply to state → upcast
   event7: deserialize patches → apply to state → upcast

7. Return final state at version 7
```

**Cost:** Snapshot load + patch application for few events. Fast for frequently-updated aggregates.

**Assumption:** Snapshot at version V means next event is at V+1 (tight versioning enforced by Store).

### Cold Path: Full Replay

```
Scenario: No snapshot, events 1-100 exist

1. SnapshotStore.Get(ctx, aggregateID)
   ↓
2. found == false: no snapshot

3. Store.ReadFrom(ctx, "events:{aggregateID}", 1)
   ↓
4. Success: event blobs at versions 1-100

5. Replayer.hydrate(zeroValue, [eventBlob1, ..., eventBlob100])
   ↓
6. Apply all 100 event patches, upcasting each to currentSchema

7. Return final state at version 100
```

**Cost:** Full event stream replay. Slow for old aggregates (cold path).

**Mitigation:** Call Preload(ctx, aggregateID) at startup, or use ShouldSnapshot() to checkpoint important versions.

### Snapshot Validation: Optimistic with Fallback

**Design Decision: Assume snapshots are valid. Only if deserialization fails, fall back to full replay.**

```go
func (r *Reader[T]) Get(ctx context.Context, aggregateID string) (T, error) {
    // Try to load snapshot — a single upserted cell, not a stream
    snapshotBlob, found, err := r.snapshotStore.Get(ctx, aggregateID)
    if err != nil {
        // Storage error, propagate
        return r.stateZeroValue, err
    }

    // Check if snapshot exists
    if !found {
        // No snapshot, full replay
        return r.coldPath(ctx, aggregateID)
    }

    // Snapshot exists, try to deserialize
    var snapshot snapshotBlob
    err = json.Unmarshal(snapshotBlob, &snapshot)
    if err != nil {
        // ❌ Deserialization failed (snapshot corrupt)
        // Fallback: Full replay from event stream
        log.Printf("snapshot deserialization failed for %s: %v, falling back to full replay",
            aggregateID, err)
        metrics.IncrementSnapshotCorruptionCount()
        return r.coldPath(ctx, aggregateID)
    }

    // ✅ Snapshot valid, deserialize state
    snapshotState, err := r.stateDeserializer(snapshot.State)
    if err != nil {
        // State deserialization failed
        log.Printf("snapshot state deserialization failed for %s: %v, falling back to full replay",
            aggregateID, err)
        metrics.IncrementSnapshotCorruptionCount()
        return r.coldPath(ctx, aggregateID)
    }

    // ✅ Snapshot version known (from snapshotBlob.Version)
    // Load events after snapshot
    internalEventBlobs, err := r.store.ReadFrom(ctx, "events:"+aggregateID, snapshot.Version+1)
    if err != nil {
        return r.stateZeroValue, err
    }

    // Deserialize internalEvent blobs
    var internalEvents []internalEvent
    for _, blob := range internalEventBlobs {
        var evt internalEvent
        err := json.Unmarshal(blob, &evt)
        if err != nil {
            return r.stateZeroValue, err
        }
        internalEvents = append(internalEvents, evt)
    }

    // Apply delta events on top of snapshot (with upcasting + auto-snapshot)
    finalState, err := r.replayer.hydrate(ctx, aggregateID, snapshotState, internalEvents)
    if err != nil {
        // Three possible errors:
        // 1. Upcaster panic → propagate (fail fast)
        // 2. Patch application error → propagate (data corruption)
        // 3. Auto-snapshot write error → return finalState + error
        //    (state is correct, snapshot is optimization only)
        // Caller (processor) treats any error as retriable
        return finalState, err
    }

    return finalState, nil
}

func (r *Reader[T]) coldPath(ctx context.Context, aggregateID string) (T, error) {
    // Full replay from version 1
    internalEventBlobs, err := r.store.ReadFrom(ctx, "events:"+aggregateID, 1)
    if err != nil {
        return r.stateZeroValue, err
    }

    if len(internalEventBlobs) == 0 {
        // No events → aggregate never existed
        return r.stateZeroValue, asynx.ErrNotFound
    }

    // Deserialize internalEvent blobs
    var internalEvents []internalEvent
    for _, blob := range internalEventBlobs {
        var evt internalEvent
        err := json.Unmarshal(blob, &evt)
        if err != nil {
            return r.stateZeroValue, err
        }
        internalEvents = append(internalEvents, evt)
    }

    // Full replay from zero value (with upcasting + auto-snapshot)
    finalState, err := r.replayer.hydrate(ctx, aggregateID, r.stateZeroValue, internalEvents)
    if err != nil {
        // Three possible errors:
        // 1. Upcaster panic → propagate (fail fast)
        // 2. Patch application error → propagate (data corruption)
        // 3. Auto-snapshot write error → return finalState + error
        //    (state is correct, snapshot is optimization only)
        // Caller (processor) treats any error as retriable
        return finalState, err
    }

    return finalState, nil
}
```

**Error Handling:**
- **Snapshot deserialization error** → Log + fallback to full replay (safe, optimistic design)
- **internalEvent deserialization error** → Propagate (data corruption)
- **Storage error** → Propagate immediately (caller handles)
- **Upcaster panic** → Propagate (fail fast, data integrity issue)
- **Auto-snapshot write error** → Return (finalState, error) — state is correct and durable; caller decides how to handle
- **No events** → ErrNotFound (aggregate never existed)

**No Version Gap Checking:** internalEvent.Version is explicit in each event, trust it. If gaps exist, Replayer will detect during hydration.

### Method: Get

```go
Get(ctx context.Context, aggregateID string) (T, error)
```

**Path Selection (Automatic):**
1. Try to load snapshot
2. If snapshot exists and deserializes: warm path (snapshot + delta)
3. If snapshot missing or corrupts: cold path (full replay)
4. Return state or error

**Guarantees:**
- Always returns latest committed state
- Strong consistency
- Transparent path selection
- Snapshot corruption handled gracefully

**Errors:**
- `ErrNotFound` — aggregate has no events
- Storage errors — propagated from Store
- Upcaster panics — propagated (schema migration error)

### Method: Exists

```go
Exists(ctx context.Context, aggregateID string) (bool, error)
```

**Implementation:**
```go
func (r *Reader[T]) Exists(ctx context.Context, aggregateID string) (bool, error) {
    // Check if any event exists (minimal query)
    events, err := r.store.ReadRange(ctx, "events:"+aggregateID, 1, 1)
    if err != nil {
        return false, err
    }

    return len(events) > 0, nil
}
```

**Guarantees:**
- Faster than Get() — only reads first event (ReadRange with count=1)
- Returns true if aggregate has any event
- Returns false if aggregate never existed

### Method: Preload

```go
Preload(ctx context.Context, aggregateID string) error
```

**Purpose:** Pay the cold path cost offline, at startup. Subsequent Get() calls will use warm path (if snapshot was created).

**Implementation:**
```go
func (r *Reader[T]) Preload(ctx context.Context, aggregateID string) error {
    // Trigger full replay, discard result
    _, err := r.Get(ctx, aggregateID)

    // ErrNotFound is OK (aggregate doesn't exist yet)
    if err != nil && err != asynx.ErrNotFound {
        return err
    }

    return nil
}
```

**Behavior:**
- Loads state via Get() (warm or cold path)
- If snapshot exists: warms load path
- If no snapshot: triggers full replay, warms cache for Replay()
- Discards result (caller doesn't care)

**Usage:**
```go
// At application startup
for _, aggregateID := range hotAggregates {
    instance.Preload(ctx, aggregateID)
}
// Now Get() calls are fast (warm path)
```

### No In-Memory Caching Strategy

**Design Decision: Reader never caches in memory.**

**Rationale:**
1. **Simplicity** — storage is source of truth, no invalidation needed
2. **Multi-node safety** — cache invalidation is hard in distributed systems
3. **Operator control** — Redis/Memcached let operators tune caching per deployment
4. **Memory bound** — no unbounded cache growth

**For High Traffic Scenarios:**

```
Problem: 1000 Gets/sec for same aggregate
  Without cache → 1000 store queries/sec → storage bottleneck

Solution 1: Preload at startup
  reader.Preload(ctx, "hot_order_123")
  First Get() warms snapshot path, subsequent Gets are fast

Solution 2: Layer Redis (operator responsibility)
  type CachedReader[T] struct {
      inner *Reader[T]
      redis redis.Client
  }

  func (cr *CachedReader[T]) Get(ctx, id string) (T, error) {
      // Try cache
      if val, err := cr.redis.Get(ctx, "order:"+id); err == nil {
          return deserialize(val), nil
      }
      // Cache miss, load from eventstore
      val, err := cr.inner.Get(ctx, id)
      if err == nil {
          // Populate cache (5 min TTL)
          cr.redis.Set(ctx, "order:"+id, serialize(val), 5*time.Minute)
      }
      return val, err
  }
```

**Guidance:**
- **Single-node, low traffic:** Use Preload() for hot aggregates
- **Single-node, high traffic:** Use Preload() + snapshot checkpoints (ShouldSnapshot())
- **Multi-node:** Layer Redis/Memcached on top of Reader
- **Multi-node, critical data:** Use both Redis + Preload()

---

## Sub-Module: Writer

### Purpose

Takes a command's state transition (old → new) and durably writes it as an event. Computes RFC 6902 diffs, optionally writes snapshots, manages save points.

### RFC 6902 Full-State Diff Strategy

**Design Decision: Always diff the entire old and new state. Never field-level reflection.**

```go
type RFC6902Computer struct {
    // Uses standard jsonpatch library or custom implementation
}

func (dc *RFC6902Computer) ComputeDiff(old T, new T) ([]jsonpatch.JsonPatchOperation, error) {
    // 1. Serialize old state to JSON
    oldJSON, err := json.Marshal(old)
    if err != nil {
        return nil, err
    }

    // 2. Serialize new state to JSON
    newJSON, err := json.Marshal(new)
    if err != nil {
        return nil, err
    }

    // 3. Compute JSON patch using jsonpatch library
    patches, err := jsonpatch.CreatePatch(oldJSON, newJSON)
    if err != nil {
        return nil, err
    }

    // 4. Return patches (may be empty if identical states)
    return patches, nil
}
```

**Example:**

```
Old: {Status: "pending", Items: 2, Total: 100}
New: {Status: "shipped", Items: 2, Total: 100}

Patches:
[
  {op: "replace", path: "/Status", value: "shipped"}
]

Empty Diff Case:
Old: {Status: "pending"}
New: {Status: "pending"}

Patches: []  ← empty, but event still written
```

**Why Not Field-Level Diffing:**
- RFC 6902 is a standard, widely understood
- Simple implementation, no reflection overhead in diffs (serialize entire state once)
- Diff size is small compared to state size for most aggregates
- Deterministic (same old/new always produces same patches)

### Writer.Write Implementation

```go
func (w *Writer[T]) Write(
    ctx context.Context,
    aggregateID string,
    eventName string,
    previousState T,
    newState T,
    version int64,
    shouldSnapshot bool,
) (Event[T], error) {
    // 1. Compute RFC 6902 patches (full state diff)
    patches, err := w.diffComputer.ComputeDiff(previousState, newState)
    if err != nil {
        return Event[T]{}, err
    }

    // 2. Create internalEvent (patches + metadata)
    eventID := generateUUID()  // Unique event ID
    internalEvent := internalEvent{
        ID:            eventID,
        EventName:     eventName,
        Version:       version,
        SchemaVersion: w.currentSchemaVersion,
        OccurredAt:    time.Now(),
        Patches:       patches,  // May be empty, that's OK
    }

    // 3. Serialize internalEvent to JSON
    eventJSON, err := json.Marshal(internalEvent)
    if err != nil {
        return Event[T]{}, err
    }

    // 4. Append internalEvent to event stream (SAVE POINT)
    err = w.store.Append(ctx, "events:"+aggregateID, version, eventJSON)
    if err != nil {
        // Uniqueness violation, storage error, etc.
        // Return ErrPipelineFailed (handled by processor)
        return Event[T]{}, err
    }
    // ✅ SAVE POINT: Event is durable, patches are safe

    // 5. Write snapshot (if command says to)
    if shouldSnapshot {
        // Serialize state to bytes
        stateBytes, err := json.Marshal(newState)
        if err != nil {
            // Serialization error, but event already durable
            return Event[T]{}, err
        }

        // Create snapshot blob (with version info)
        snapshot := snapshotBlob{
            Version:       version,
            SchemaVersion: w.currentSchemaVersion,
            State:         stateBytes,
        }

        snapshotJSON, err := json.Marshal(snapshot)
        if err != nil {
            return Event[T]{}, err
        }

        // Upsert the snapshot — replaces any prior snapshot for this aggregate
        err = w.snapshotStore.Put(ctx, aggregateID, version, snapshotJSON)
        if err != nil {
            // Snapshot write failed, but event is safe
            // Return error (operator may need to investigate)
            return Event[T]{}, err
        }
    }

    // 6. Return public Event[T] for bus publish
    return Event[T]{
        ID:                eventID,
        AggregateID:       aggregateID,
        EventName:         eventName,
        Version:           version,
        SchemaVersion:     w.currentSchemaVersion,
        OccurredAt:        internalEvent.OccurredAt,
        Aggregate:         newState,
        PreviousAggregate: previousState,
    }, nil
}
```

**Key Points:**
- internalEvent is serialized to Store (opaque bytes to Store)
- Public Event[T] is returned to caller for bus publish
- Snapshot includes version info (no guessing needed by Reader)
- Empty patches are allowed and stored

### Empty Diffs: Allowed and Written

**Design Decision: Allow events with empty patch lists.**

```go
func (w *Writer[T]) Write(
    ctx context.Context,
    aggregateID string,
    eventName string,
    previousState T,
    newState T,
    version int64,
    shouldSnapshot bool,
) (Event[T], error) {
    // 1. Compute diff
    patches, err := w.diffComputer.ComputeDiff(previousState, newState)
    if err != nil {
        return Event[T]{}, err
    }

    // 2. Serialize patches (may be empty)
    patchesJSON, err := json.Marshal(patches)  // Could be "[]"
    if err != nil {
        return Event[T]{}, err
    }

    // 3. Write event (always, even if patches empty)
    err = w.store.Append(ctx, "events:"+aggregateID, version, patchesJSON)
    if err != nil {
        // Uniqueness violation, storage error, etc.
        return Event[T]{}, err
    }
    // ✅ SAVE POINT: Event is durable

    // 4. Write snapshot (only if command says so)
    if shouldSnapshot {
        snapshotJSON, err := json.Marshal(newState)
        if err != nil {
            // Serialization error, but event already durable
            return Event[T]{}, err
        }

        err = w.snapshotStore.Put(ctx, aggregateID, version, snapshotJSON)
        if err != nil {
            // Snapshot write failed, but event is safe
            // Return error (operator may need to investigate)
            return Event[T]{}, err
        }
    }

    // 5. Return event with full state
    return Event[T]{
        ID:                generateUUID(),
        AggregateID:       aggregateID,
        EventName:         eventName,
        Version:           version,
        SchemaVersion:     w.currentSchemaVersion,
        OccurredAt:        time.Now(),
        Aggregate:         newState,
        PreviousAggregate: previousState,
    }, nil
}
```

**Why Allow Empty Diffs:**
- Some commands are idempotent (apply twice = apply once)
- Event is still meaningful (audit trail: command was received and processed)
- No different from a one-line patch
- Cost is minimal (empty JSON array is 2 bytes)
- Simpler than checking "is diff empty?" before writing

**Example: Idempotent Command**
```
Scenario: AddTag command is retried (caller didn't see response)

Command 1: AddTag("important") → state has ["important"]
Patches: [insert /tags/0 "important"]
Event written

Command 2: AddTag("important") → state still ["important"]
Patches: []  ← empty, but event written (idempotence marker)
Event written
```

### Snapshot Writing Strategy

**Design Decision: Snapshot only when command explicitly says so (ShouldSnapshot()=true).**

```go
// In command implementation
type CheckoutOrderCmd struct {
    OrderID string
}

func (c *CheckoutOrderCmd) ShouldSnapshot() bool {
    // Checkout is important checkpoint
    return true
}

// Or

type AddItemCmd struct {
    OrderID string
    Item    string
}

func (c *AddItemCmd) ShouldSnapshot() bool {
    // Adding items doesn't need checkpoint
    return false
}
```

**Timeline:**
```
Version 1: CreateOrder → ShouldSnapshot()=false
Version 2: AddItem → ShouldSnapshot()=false
Version 3: AddItem → ShouldSnapshot()=false
Version 4: Checkout → ShouldSnapshot()=true
   ↓ Snapshot written at version 4

Next Get():
  Load snapshot at version 4 (warm path)
  Apply any events at 5+ (delta)
  Warm path from then on
```

**Guidance:**
- Snapshot after important state transitions (lifecycle events)
- Don't snapshot after every event (storage bloat)
- Let command logic decide (domain-driven)
- Example thresholds:
  - CreateOrder: snapshot
  - Checkout: snapshot
  - AddItem: no snapshot (until checkout)

---

## Sub-Module: Replayer

### Purpose

Iterates events in version order, applies patches to hydrate state, upcasts to schema migrations, and supports manual projection recovery.

### Hydration: Applying Patches, Upcasting, and Auto-Snapshots

**Design Decision: Hydrate is self-contained — applies patches + upcasts + writes auto-snapshots.**

```go
func (r *Replayer[T]) hydrate(
    ctx context.Context,
    aggregateID string,
    seedState T,
    internalEvents []internalEvent,
) (T, error) {
    current := seedState
    upcasted := false  // Track if any upcasting happened
    var lastVersion int64

    for i, evt := range internalEvents {
        // 1. Check if this event needs upcasting
        if evt.SchemaVersion < r.currentSchemaVersion {
            upcasted = true
        }

        // 2. Upcast internalEvent to current schema
        upcastedEvt, err := r.upcastInternalEvent(evt)
        if err != nil {
            return r.stateZeroValue, err
        }

        // 3. Apply patches to current state
        current, err = r.applyPatches(current, upcastedEvt.Patches)
        if err != nil {
            return r.stateZeroValue, err
        }

        lastVersion = evt.Version
    }

    // 4. If upcasting happened, seal the migration by writing a snapshot
    // (only if events were actually processed)
    if upcasted && len(internalEvents) > 0 {
        err := r.writeAutoSnapshot(ctx, aggregateID, lastVersion)
        if err != nil {
            // Auto-snapshot failed, but state is correct
            // Propagate error (operator should investigate)
            return r.stateZeroValue, err
        }
    }

    return current, nil
}

func (r *Replayer[T]) applyPatches(state T, patches []jsonpatch.JsonPatchOperation) (T, error) {
    // 1. Serialize current state to JSON
    currentJSON, err := json.Marshal(state)
    if err != nil {
        return r.stateZeroValue, err
    }

    // 2. Apply RFC 6902 patches
    patchedJSON, err := jsonpatch.ApplyPatch(currentJSON, patches)
    if err != nil {
        return r.stateZeroValue, err
    }

    // 3. Deserialize patched JSON back to state
    var patchedState T
    err = json.Unmarshal(patchedJSON, &patchedState)
    if err != nil {
        return r.stateZeroValue, err
    }

    return patchedState, nil
}

func (r *Replayer[T]) writeAutoSnapshot(
    ctx context.Context,
    aggregateID string,
    version int64,
) error {
    // Serialize current state (but we don't have it here...)
    // This is the challenge: Replayer needs to remember final state
    // OR: This method should be called with final state
}
```

**Wait — there's an issue:** In hydrate, we apply patches sequentially but discard each intermediate state. To write auto-snapshot at the end, we need the final state. Let me refactor:

```go
func (r *Replayer[T]) hydrate(
    ctx context.Context,
    aggregateID string,
    seedState T,
    internalEvents []internalEvent,
) (T, error) {
    current := seedState
    upcasted := false
    var lastVersion int64

    for _, evt := range internalEvents {
        // Check if upcasting needed
        if evt.SchemaVersion < r.currentSchemaVersion {
            upcasted = true
        }

        // Upcast internalEvent
        upcastedEvt, err := r.upcastInternalEvent(evt)
        if err != nil {
            return r.stateZeroValue, err
        }

        // Apply patches to state
        current, err = r.applyPatches(current, upcastedEvt.Patches)
        if err != nil {
            return r.stateZeroValue, err
        }

        lastVersion = evt.Version
    }

    // If upcasting happened, seal the migration
    if upcasted && len(internalEvents) > 0 {
        // Write snapshot with final state
        snapshot := snapshotBlob{
            Version:       lastVersion,
            SchemaVersion: r.currentSchemaVersion,
            State:         r.serializeState(current),  // ← Need serializer
        }

        snapshotJSON, _ := json.Marshal(snapshot)
        err := r.snapshotStore.Put(ctx, aggregateID, lastVersion, snapshotJSON)
        if err != nil {
            // Auto-snapshot failed (storage issue, not data corruption)
            // State is correct and durable; snapshot is optimization only
            // Return BOTH the error AND the hydrated state
            // Caller (Reader.Get) decides whether to propagate error or succeed
            return current, err
        }
    }

    return current, nil
}
```

**Better design:** Replayer should have:
- `store` — for writing auto-snapshots
- `stateSerializer` — for serializing final state to snapshot blob
- The auto-snapshot write is automatic and transparent

### Always-Upcast Design

**Design Decision: Every event yielded by Replayer is upcasted to currentSchemaVersion.**

Upcasting happens at the `internalEvent` level (before state hydration), so both Reader and Replay benefit from the same logic.

```go
func (r *Replayer[T]) upcastInternalEvent(evt internalEvent) (internalEvent, error) {
    // Apply upcaster chain: SchemaVersion → ... → currentSchemaVersion
    for v := evt.SchemaVersion; v < r.currentSchemaVersion; v++ {
        upcaster, ok := r.upcasters[v]
        if !ok {
            // No upcaster for this step, skip
            continue
        }

        // Apply upcaster (transforms internalEvent from schema V to V+1)
        // If upcaster panics, panic propagates (no recover, fail fast)
        upcasted, err := upcaster(evt)
        if err != nil {
            return internalEvent{}, err
        }

        evt = upcasted
    }

    // evt now at currentSchemaVersion
    return evt, nil
}

func (r *Replayer[T]) Replay(
    ctx context.Context,
    aggregateID string,
    fromVersion int64,
    toVersion int64,
    fn func(Event[T]),
) error {
    // Read event blobs
    eventBlobs, err := r.store.ReadRange(ctx, "events:"+aggregateID, fromVersion, toVersion)
    if err != nil {
        return err
    }

    // Hydrate state and create Event[T] objects
    current := r.stateZeroValue
    for _, eventBlob := range eventBlobs {
        // 1. Deserialize internalEvent from blob
        var internalEvt internalEvent
        err := json.Unmarshal(eventBlob, &internalEvt)
        if err != nil {
            return err
        }

        previous := current

        // 2. Upcast internalEvent to current schema
        upcastedEvt, err := r.upcastInternalEvent(internalEvt)
        if err != nil {
            return err  // Fail fast on upcaster error
        }

        // 3. Apply patches to get new state
        current, err = r.applyPatches(current, upcastedEvt.Patches)
        if err != nil {
            return err
        }

        // 4. Create public Event[T] with full state
        event := Event[T]{
            ID:                upcastedEvt.ID,
            AggregateID:       aggregateID,
            EventName:         upcastedEvt.EventName,
            Version:           upcastedEvt.Version,
            SchemaVersion:     upcastedEvt.SchemaVersion,  // Now at currentSchemaVersion
            OccurredAt:        upcastedEvt.OccurredAt,
            Aggregate:         current,
            PreviousAggregate: previous,
        }

        // 5. Yield to callback
        fn(event)
    }

    return nil
}
```

**Behavior:**
- internalEvent read at SchemaVersion 1
- Upcasted 1→2 (if upcaster registered)
- Upcasted 2→3 (if upcaster registered)
- Patches applied (from upcasted event)
- Public Event[T] yielded at currentSchemaVersion

**Callbacks see consistent schema:**
```go
instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    // e.Aggregate is at currentSchemaVersion
    // Even if original internalEvent was at SchemaVersion 1
    // Callback never needs to check SchemaVersion

    if e.Aggregate.Status == "shipped" {
        notifyCustomer(e.AggregateID)
    }
})
```

**Important: Replay Never Auto-Snapshots**

Even though Replay uses upcastInternalEvent, it never writes a snapshot:

```go
// Replay is read-only, for manual recovery
instance.Replay(ctx, aggregateID, 0, 0, fn)  // No snapshots written
```

Auto-snapshots are written only by `hydrate()` (called from Reader.Get/warmPath/coldPath).

### Upcaster Chain: How It Works

**Design:** Upcasters operate on `internalEvent` (not public Event[T]), transforming patches + metadata from one schema to the next.

**Registration (on builder):**
```go
asynx.New[Order]().
    WithSchemaVersion(3).
    WithUpcaster(1, func(evt internalEvent) (internalEvent, error) {
        // Transform internalEvent from SchemaVersion 1 → 2
        // Patches may need adjustment if fields renamed/removed
        // Or patches unchanged if additive-only changes
        return evt, nil
    }).
    WithUpcaster(2, func(evt internalEvent) (internalEvent, error) {
        // Transform internalEvent from SchemaVersion 2 → 3
        return evt, nil
    }).
    Build()
```

**Execution Flow:**
```
internalEvent at SchemaVersion 1:
  Upcaster[1](evt) → internalEvent at SchemaVersion 2
    ↓
  Upcaster[2](evt) → internalEvent at SchemaVersion 3
    ↓
  Apply patches to state
  Yield public Event[T]

internalEvent at SchemaVersion 2:
  Upcaster[2](evt) → internalEvent at SchemaVersion 3
    ↓
  Apply patches to state
  Yield public Event[T]

internalEvent at SchemaVersion 3:
  No upcasters needed
  Apply patches to state
  Yield public Event[T]
```

**Invariant:** Upcaster[i] expects input at SchemaVersion i and produces SchemaVersion i+1. Upcasters are responsible for transforming patches to match the new schema.

**Example: Rename field (Status → OrderStatus)**

```go
// Old schema (SchemaVersion 1):
// {Status: "pending"}

// New schema (SchemaVersion 2):
// {OrderStatus: "pending"}  ← Field renamed

WithUpcaster(1, func(evt internalEvent) (internalEvent, error) {
    // Patches from old schema:
    // [{op: "replace", path: "/Status", value: "shipped"}]

    // Transform to new schema:
    // [{op: "replace", path: "/OrderStatus", value: "shipped"}]

    var patches []jsonpatch.JsonPatchOperation
    json.Unmarshal(evt.Patches, &patches)

    for _, patch := range patches {
        if patch.Path == "/Status" {
            patch.Path = "/OrderStatus"  // Rename field path
        }
    }

    evt.Patches = newPatches
    evt.SchemaVersion = 2  // Update version
    return evt, nil
})
```

### Upcaster Chain Failure: Fail Fast

**Design Decision: If upcaster errors or panics, fail fast. Don't skip or recover.**

```go
func (r *Replayer[T]) upcastEvent(event Event[T]) (Event[T], error) {
    for v := event.SchemaVersion; v < r.currentSchemaVersion; v++ {
        upcaster, ok := r.upcasters[v]
        if !ok {
            continue
        }

        // Call upcaster
        // If it panics, panic propagates (no defer recover)
        // If it returns error, return error to caller
        upcasted, err := upcaster(event)
        if err != nil {
            // Explicit error from upcaster
            return Event[T]{}, fmt.Errorf("upcaster %d failed: %w", v, err)
        }

        event = upcasted
    }

    return event, nil
}
```

**Why Fail Fast:**
- Schema migrations are developer responsibility (custom logic)
- If upcaster fails, data corruption is likely
- Better to alert operator immediately than silently skip
- Replay() is manual recovery, not auto-recovery

**Example: Upcaster Panic**
```go
// Register upcaster for 1→2
builder.WithUpcaster(1, func(e asynx.Event[Order]) (asynx.Event[Order], error) {
    // Panics if order total is negative (data corruption)
    if e.Aggregate.Total < 0 {
        panic("corrupted order: negative total")
    }

    // Upcast logic: rename field
    e.Aggregate.OrderStatus = e.Aggregate.Status  // new field
    return e, nil
})

// Call Replay()
err := instance.Replay(ctx, "order_123", 0, 0, func(e asynx.Event[Order]) {
    // Process event...
})

// If event has Total < 0:
//   Upcaster panics → panic propagates → Replay returns error
//   Operator sees it, investigates
```

### Auto-Snapshot After Upcasting (in Hydrate, Not Replay)

When `hydrate()` loads old events and upcasts them, it automatically writes a snapshot to "seal the migration."

**Design Decision: Replayer.hydrate() auto-snapshots when upcasting occurs.**

This is implemented directly in hydrate:

```go
func (r *Replayer[T]) hydrate(
    ctx context.Context,
    aggregateID string,
    seedState T,
    internalEvents []internalEvent,
) (T, error) {
    current := seedState
    upcasted := false
    var lastVersion int64

    for _, evt := range internalEvents {
        // Track if any event needed upcasting
        if evt.SchemaVersion < r.currentSchemaVersion {
            upcasted = true
        }

        // Upcast and apply patches
        upcastedEvt, err := r.upcastInternalEvent(evt)
        if err != nil {
            return r.stateZeroValue, err
        }

        current, err = r.applyPatches(current, upcastedEvt.Patches)
        if err != nil {
            return r.stateZeroValue, err
        }

        lastVersion = evt.Version
    }

    // If upcasting happened, seal the migration by writing snapshot
    if upcasted && len(internalEvents) > 0 {
        snapshot := snapshotBlob{
            Version:       lastVersion,
            SchemaVersion: r.currentSchemaVersion,  // Current version
            State:         r.serializeState(current),
        }

        snapshotJSON, _ := json.Marshal(snapshot)
        err := r.snapshotStore.Put(ctx, aggregateID, lastVersion, snapshotJSON)
        if err != nil {
            // Auto-snapshot failed, but state is correct and durable
            // Log and metric, but don't block hydration
            // Next load may re-attempt snapshot write
            log.Printf("auto-snapshot write failed for %s at version %d: %v",
                aggregateID, lastVersion, err)
            metrics.IncrementAutoSnapshotFailureCount()
            // Continue anyway - state is already correct
        }
    }

    return current, nil
}
```

**Rationale:**
- First time an old event is loaded, it's upcasted (possibly expensive)
- Snapshot "seals the migration" — subsequent loads use warm path (snapshot+delta)
- Cost paid once per aggregate, then fast path forever
- Happens automatically inside hydrate (transparent to caller)

**Call Paths:**
- **Reader.warmPath → hydrate()** — auto-snapshot after upcasting ✅
- **Reader.coldPath → hydrate()** — auto-snapshot after upcasting ✅
- **Replay() → upcastInternalEvent()** — NO snapshot (read-only) ✅

**Important: Replay() Never Auto-Snapshots**

Replay() is for manual recovery and uses upcastInternalEvent, but it never calls hydrate (which writes snapshots). This keeps the snapshot stream clean — only explicit commands and Reader auto-migrations write snapshots.

---

## Interaction Between Sub-Modules

### Command Execution Flow (Full Pipeline)

```
Processor.Send(cmd)
  ↓
→ Reader.Get(aggregateID)
   Load snapshot or full replay
   Return state (upcasted if needed)

  ↓
  cmd.Validate(currentState)
  cmd.EmitEvent(currentState)

  ↓
→ Writer.Write(aggregateID, eventName, old, new, version, shouldSnapshot)
   Compute RFC 6902 diff
   Serialize patches
   Append event to store (SAVE POINT)
   If shouldSnapshot: append snapshot

  ↓
→ Bus.Publish(event)
   Dispatch to subscriptions (async)
```

### Snapshot Out-of-Sync Recovery

**Scenario: Snapshot version doesn't match event stream**

```
Snapshot exists at version 5
Event stream has versions 1-7
Partial write crash: snapshot written, but event 7 not written

Reader detects this:
  Load snapshot at version 5
  Try to load events from 6+
  Event 6 exists, event 7 missing → version gap detected

Recovery: Use replayer to fill gap
  Get last version from events (6)
  Apply patches to snapshot state
  Return state at version 6
```

**Implementation:**
```go
func (r *Reader[T]) warmPath(ctx, aggregateID, snapshotVersion) (T, error) {
    // Load snapshot
    snapshotState, _ := loadSnapshot(...)

    // Load events after snapshot
    eventBlobs, _ := r.store.ReadFrom(ctx, "events:"+aggregateID, snapshotVersion+1)

    // If events exist, apply them
    // If gap detected (e.g., version mismatch), full replay is safer
    if len(eventBlobs) == 0 {
        // No events after snapshot, safe
        return snapshotState, nil
    }

    // Apply patches
    finalState, err := r.replayer.hydrate(snapshotState, eventBlobs)
    return finalState, err
}
```

---

## Error Handling

### Reader Errors

| Error | Cause | Handler |
|-------|-------|---------|
| ErrNotFound | No events exist | Aggregate never created |
| Storage error | ReadFrom/ReadRange failed | Propagate to caller |
| Deserialization error | Event blob malformed | Propagate (data corruption) |
| Snapshot deserialize error | Snapshot corrupted | Fall back to full replay |
| Upcaster panic | Migration logic failed | Propagate (fail fast) |

### Writer Errors

| Error | Cause | Handler |
|-------|-------|---------|
| Uniqueness violation | Version conflict (multi-node) | ErrPipelineFailed to caller |
| Storage error | Append failed | Propagate to caller |
| Serialization error | State not JSON-serializable | Propagate (caller bug) |
| Diff computation error | Old/new state incompatible | Propagate (logic error) |
| Snapshot write error | Snapshot Append failed | Event already safe, return error |

### Replayer Errors

| Error | Cause | Handler |
|-------|-------|---------|
| Storage error | ReadRange failed | Propagate to caller |
| Deserialization error | Event blob malformed | Propagate (data corruption) |
| Patch application error | Patches don't apply to state | Propagate (data integrity) |
| Upcaster panic | Migration logic panicked | Propagate (fail fast) |
| Upcaster error | Upcaster returned error | Propagate (schema migration failed) |

---

## Concurrency & Thread Safety

### Reader: Thread-Safe, No Caching

- Multiple goroutines can call Get() concurrently
- Each Get() queries storage independently
- No shared state (no cache)
- Thread-safe as long as underlying Store is thread-safe

### Writer: Thread-Safe (Caller Coordination)

- Multiple goroutines can call Write() concurrently
- Store.Append enforces (aggregateID, version) uniqueness
- Version conflicts are detected by Store, not Writer
- Thread-safe as long as underlying Store is thread-safe

### Replayer: Thread-Safe, Read-Only

- Multiple goroutines can call Replay() concurrently
- No mutations, no side effects
- Upcasters must be stateless or thread-safe
- Thread-safe as long as underlying Store is thread-safe

---

## Memory & Performance Considerations

### Cold Path Cost

Aggregates with long histories pay a cost on first access:

```
Scenario: Aggregate with 10,000 events, no snapshot

Get() →
  Load all 10,000 events from storage
  Deserialize patches for each
  Apply to state sequentially
  May take seconds

Mitigation:
  1. Preload(ctx, aggregateID) at startup (pay cost offline)
  2. Use ShouldSnapshot() to checkpoint important versions
  3. Layer Redis to cache loaded states
```

### Snapshot Strategy Impact

| Strategy | Storage | Load Time | First Access Cost |
|----------|---------|-----------|-------------------|
| No snapshots | Minimal | Cold path always (slow) | 10+ seconds |
| Some snapshots | Medium | Mixed warm/cold | Depends on checkpoints |
| Many snapshots | High | Mostly warm path (fast) | <100ms |

**Recommendation:** Start with some snapshots (important state transitions), monitor, adjust.

### Diff Size Impact

RFC 6902 patches are typically small:

```
Order with 10 fields, 1 changes:
Patch: [{op: "replace", path: "/status", value: "shipped"}] → ~50 bytes

Order with 100 nested items, 1 changes:
Patch: [{op: "replace", path: "/items/5/quantity", value: 10}] → ~60 bytes
```

Patches are usually much smaller than full state.

---

## Configuration via Builder

```go
instance, err := asynx.New[Order]().
    WithEventStore(eventStore).
    WithSnapshotStore(snapshotStore).
    WithSchemaVersion(2).
    WithUpcaster(1, migrateV1toV2).
    Build()
```

**Schema Version:**
- Bumped only for destructive changes (rename, remove, type change)
- Additive changes (new fields with defaults) don't need bump

**Upcasters:**
- Chainable (1→2→3 → current)
- Registered in order of application
- Must be idempotent (replaying same event multiple times is safe)

---

## Testing Strategy

### Unit Tests

- **Reader warm path:** Mock snapshot and events, verify state reconstruction
- **Reader cold path:** Mock only events, verify full replay
- **Reader snapshot corruption:** Mock corrupted snapshot, verify fallback
- **Writer full diff:** Verify RFC 6902 patches are correct
- **Writer empty diff:** Verify empty patch list is allowed
- **Replayer upcasting:** Verify upcasters are applied in order
- **Replayer upcaster panic:** Verify panic propagates

### Integration Tests

- **End-to-end Get/Write cycle:** Real Store, verify state is recoverable
- **Snapshot warm path:** Create snapshots, verify warm path is used
- **Schema migration:** Write old events, add upcaster, verify migrated on load
- **Concurrent Reads:** Multiple goroutines Get() same aggregate
- **Concurrent Writes:** (Processor handles ordering, writer just validates)

### Benchmarks

- **Get() warm path:** <5ms (snapshot + few events)
- **Get() cold path:** <1s (100 events), <10s (1000 events)
- **Preload():** Background load, non-blocking
- **Replay():** Iterate all events efficiently

---

## Known Gotchas

### 1. Snapshot Version Must Be Explicit

Events don't store their version (version is key in Store, not in blob). When loading snapshot, you must infer version from Store's key or store it in snapshot metadata.

```go
// Option A: Store version in snapshot blob
type Snapshot struct {
    State T
    Version int64
}

// Option B: Store version in metadata separately
// (depends on Store implementation)
```

### 2. Upcasters Are Not Automatic

Bumping SchemaVersion without registering upcasters will crash Replay():

```go
// ❌ Wrong
asynx.New[Order]().
    WithSchemaVersion(2).  // Bumped version
    // Oops, forgot to register upcaster
    Build()

// Later, Replay() crashes trying to find upcaster[1]

// ✅ Right
asynx.New[Order]().
    WithSchemaVersion(2).
    WithUpcaster(1, migrateV1toV2).  // Registered
    Build()
```

### 3. RFC 6902 Patches Are Not Idempotent

Applying same patch twice can give different results:

```
Patch: [{op: "remove", path: "/status"}]

State 1: {status: "pending"} → Apply → {status: missing} ✓
State 2: {status: missing} → Apply → Error (path doesn't exist) ✗
```

**Implication:** Replay() must always apply patches in order, never skip or repeat.

### 4. Empty Diffs Can Mask Bugs

If a command's EmitEvent returns unchanged state, the event is written with empty patches. This is allowed but might be a bug:

```go
func (cmd *UpdateOrderCmd) EmitEvent(current *Order) Order {
    // Oops, forgot to actually update anything
    return *current  // Same as input
}

// Result: Empty patch event is written
// Replay shows command was processed, but nothing changed
```

Recommend logging/metrics for empty diff events.

---

## Summary

The eventstore uses a **three-part architecture**:

- **Reader** — Optimistic snapshot validation, warm/cold paths, no in-memory caching
- **Writer** — RFC 6902 full-state diffs, snapshot on command demand
- **Replayer** — Always-upcast events, hydrate applies patches, auto-snapshots on upcasting

### Key Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Event storage format | internalEvent (patches + metadata) | Efficient, separates public Event[T] from storage format |
| Snapshot version | In snapshotBlob metadata | Reader knows version, no guessing |
| Snapshot validation | Optimistic (assume valid, fallback on error) | Fast path, graceful recovery |
| Caching strategy | No in-memory cache (operators layer Redis) | Simplicity, multi-node safety |
| Diff computation | Full-state RFC 6902 | Standard, deterministic, no reflection |
| Empty diffs | Allowed and written | Idempotence markers, minimal cost |
| Upcasting location | In Replayer, applies to internalEvent | Single point of logic, used by all paths |
| Auto-snapshot trigger | Hydrate detects upcasting, writes directly | Self-contained, seals migration once |
| Replay snapshots | Never (read-only) | Keeps the stored snapshot clean |
| Snapshot persistence | Separate `SnapshotStore` interface (`Put`/`Get`/`Delete`), one upserted row per aggregate — not a `Store` stream | O(1) read/write regardless of snapshot history; `Store` keeps `events:id` naming, `SnapshotStore` needs no prefix |

### Type Relationships

```
Public API (in core):
  Event[T]  ← Seen by projection callbacks, processor returns this

Internal to eventstore:
  internalEvent  ← What's actually stored in Store (patches + metadata)
  snapshotBlob   ← What's stored in SnapshotStore (version + state blob), one per aggregate

Reader:
  Deserializes internalEvent
  Calls Replayer.hydrate()
  Returns aggregate state T

Writer:
  Creates internalEvent from command
  Serializes internalEvent, appends to Store (events)
  Serializes snapshotBlob, upserts to SnapshotStore (Put) if shouldSnapshot
  Returns public Event[T] for bus

Replayer:
  Deserializes internalEvent
  Upcasts internalEvent (patches + metadata)
  Applies patches to state
  Hydrate writes auto-snapshots
  Replay yields public Event[T]
```

All three are **thread-safe, read-heavy, and storage-efficient**. Developers layer caching and performance tuning on top as needed.

# Store Package Specification

## Overview

The `store` package defines the `Store` interface — the raw persistence contract for event and snapshot streams. Store is the only durable boundary in Asynx. It accepts three operations: append a blob to a stream, read all entries from a version, and read a bounded range.

The Store interface is intentionally minimal — three methods, raw bytes, no opinions about serialization or schema. Asynx owns what goes into the blobs (RFC 6902 diffs, full snapshots); the developer owns how and where they are stored. Developers implement Store for whatever infrastructure they have: SQLite, Postgres, Redis, DynamoDB, cloud storage, etc.

The critical responsibility is enforcing the `(aggregateID, version)` uniqueness constraint — this is the only coordination mechanism needed for multi-node consistency.

## Types & Interfaces

### `Store` Interface

The stream persistence contract. Append-only semantics, no updates or deletes. Two independent Store instances can be used — one for events, one for snapshots — or a single store can handle both.

```go
type Store interface {
    // Append writes a single entry to the named stream at the given version.
    Append(ctx context.Context, aggregateID string, version int64, data []byte) error

    // ReadFrom returns all entries in the named stream from fromVersion onward.
    ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error)

    // ReadRange returns up to count entries from the stream starting at fromVersion.
    ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error)
}
```

### Method: `Append`

**Signature**
```go
Append(ctx context.Context, aggregateID string, version int64, data []byte) error
```

**Purpose**
Writes a single entry to the stream for the given aggregate at the specified version. Entries are always appended; never overwritten or updated.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate this entry belongs to (non-empty string)
  - Asynx will use stream names like `events:{aggregateID}` or `snapshots:{aggregateID}` (stream naming is Asynx's concern, not the store's)
- `version` — the monotonically increasing version for this aggregate (must be >= 1)
- `data` — the blob to store (could be RFC 6902 diff, full snapshot, or any bytes)

**Return Values**
- `error` — non-nil if append failed

**Invariants**
- **Append is idempotent on success** — appending the same (aggregateID, version, data) twice is equivalent to appending once (or errors the second time)
- **Uniqueness constraint: (aggregateID, version) pairs are unique** — the developer's implementation MUST enforce this
  - If two writes race to the same (aggregateID, version), one succeeds and one fails
  - This constraint is the atomicity guarantee that makes multi-node coordination safe
  - Typical SQL implementation uses a PRIMARY KEY or UNIQUE constraint
- **Entries are never overwritten** — attempting to append a different `data` blob at an existing (aggregateID, version) must fail
- **Version numbers are tight** — no gaps (1, 2, 3... never 1, 3, 5)
  - This is enforced by Asynx at the processor level: version conflicts are detected by the store and cause ErrPipelineFailed
- **Data is stored exactly as provided** — no corruption, no compression, no transformation by the store itself

**Side Effects**
- Durably writes entry to stable storage
- May trigger replication to other nodes (multi-node stores)
- May update indexes or caches internal to the store

**Error Handling**

Several error cases:

1. **Uniqueness violation** — (aggregateID, version) already exists
   - Return error
   - Asynx interprets this as `ErrPipelineFailed`
   - Caller retries from scratch with fresh state

2. **Context cancelled or deadline exceeded**
   - Return context error
   - Asynx propagates this to caller

3. **Storage unavailable** — disk full, connection lost, permissions denied
   - Return error
   - Caller decides: retry, fail, alert

4. **Corrupt data** — data blob is malformed (after-the-fact validation)
   - This is rarely detected by the store (it just stores bytes)
   - If detected, return error; caller cannot recover

**Example**
```go
// Asynx writes an event:
err := store.Append(ctx, "order_123", 1, rfcDiffBlob)
if err != nil {
    // Could be uniqueness violation, context cancelled, or storage error
    // Asynx decides what to do based on error type
}

// Asynx writes a snapshot (to the same or different store):
err := store.Append(ctx, "order_123", 5, fullSnapshotBlob)
if err != nil {
    // Same error handling as above
}
```

---

### Method: `ReadFrom`

**Signature**
```go
ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error)
```

**Purpose**
Returns all entries in the stream starting from `fromVersion` through to the latest entry. Used by eventstore reader to hydrate aggregate state (both warm and cold paths) and by the replayer to iterate the full stream.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to read from (non-empty string)
- `fromVersion` — inclusive starting version (>= 0)
  - `0` means start from the very first entry (version 1)
  - `5` means start from version 5

**Return Values**
- `[][]byte` — slice of blobs in strict ascending version order
  - If no entries exist at or after fromVersion, returns empty slice (not error)
  - Blobs are in the exact order they were appended (version order)
  - Each blob's version is implicitly `fromVersion + i` for position `i`
- `error` — non-nil if read failed (storage unavailable, context cancelled, etc.)

**Invariants**
- **Entries are in strict version order** — blob[0] is version fromVersion, blob[1] is version fromVersion+1, etc.
- **No gaps** — if fromVersion=5 and 3 blobs are returned, the versions are 5, 6, 7 (never 5, 6, 8)
- **Latest entry is included** — reads through to the end of the stream (no cutoff before the last entry)
- **Read is consistent** — if a stream has versions 1-10 at read time, either all of them are returned, or none are (no partial reads of the latest version)
  - In practice, eventually-consistent stores may return a view from slightly in the past

**Side Effects**
- None — pure read operation
- May trigger replication or consistency waits (implementation detail)

**Error Handling**

1. **Entry not found** — aggregateID doesn't exist or has no entries at/after fromVersion
   - Return empty slice, no error
   - Asynx interprets empty result as "aggregate doesn't exist yet" or "no new events"

2. **Storage unavailable**
   - Return error
   - Caller decides: retry, fail, alert

3. **Context cancelled or deadline exceeded**
   - Return context error
   - Caller handles timeout

**Example**
```go
// Asynx reads all events for an aggregate from the start:
entries, err := store.ReadFrom(ctx, "order_123", 0)
if err != nil {
    log.Fatal(err)
}

if len(entries) == 0 {
    // Aggregate doesn't exist yet
    return ErrNotFound
}

// entries[0] is version 1, entries[1] is version 2, etc.
for i, blob := range entries {
    version := int64(i) + 1
    applyDiff(aggregate, blob, version)
}

// Asynx also uses ReadFrom to read only delta events after a snapshot:
// If snapshot is at version 5, read starting from version 6:
deltas, err := store.ReadFrom(ctx, "order_123", 6)
// deltas[0] is version 6, deltas[1] is version 7, etc.
```

---

### Method: `ReadRange`

**Signature**
```go
ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error)
```

**Purpose**
Like `ReadFrom`, but returns at most `count` entries instead of the entire stream. Used when the caller knows exactly how many entries it needs (e.g., `Replay()` with a specific version range).

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to read from (non-empty string)
- `fromVersion` — inclusive starting version (>= 0)
- `count` — maximum number of entries to return (>= 0)
  - `0` means return 0 entries (unusual but valid)
  - `1` means return at most 1 entry
  - `1000` means return at most 1000 entries

**Return Values**
- `[][]byte` — slice of at most `count` blobs in strict ascending version order
  - Returns fewer than `count` if the stream ends before `count` entries are available
  - Returns empty slice if no entries exist at/after fromVersion
- `error` — non-nil if read failed

**Invariants**
- **Entries are in strict version order** — same as ReadFrom
- **No gaps** — same as ReadFrom
- **Length is at most count** — `len(result) <= count`
- **Consistency** — reads up to version fromVersion + count - 1 (inclusive)

**Side Effects**
- None — pure read operation

**Error Handling**
- Same as ReadFrom (no entries, storage error, context cancelled)

**Example**
```go
// Asynx replays events from version 10 to version 15 (inclusive):
// That's 6 entries: versions 10, 11, 12, 13, 14, 15
count := int64(15 - 10 + 1)  // 6
entries, err := store.ReadRange(ctx, "order_123", 10, count)

// entries should have 6 blobs (if they exist)
// entries[0] is version 10, entries[5] is version 15

// Pagination: read first 100, then next 100, etc.
entries, _ := store.ReadRange(ctx, "order_123", 1, 100)
// Got versions 1-100

entries, _ := store.ReadRange(ctx, "order_123", 101, 100)
// Got versions 101-200

entries, _ := store.ReadRange(ctx, "order_123", 201, 100)
// Got versions 201-300, but may be fewer if stream ends before 300
```

---

## Stream Naming Convention

Asynx owns stream naming. The developer's Store implementation receives aggregateID and implicitly handles two stream families:

```
events:{aggregateID}     → append-only stream of RFC 6902 patches
snapshots:{aggregateID}  → append-only stream of full aggregate state snapshots
```

The developer's implementation receives `aggregateID` (e.g., `"order_123"`) and the actual stream name (e.g., `"events:order_123"`) is constructed by Asynx before calling Append/ReadFrom/ReadRange.

**The Store interface never sees the "events:" or "snapshots:" prefix — that's Asynx's concern.**

Typical SQL implementation:

```sql
CREATE TABLE events (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    PRIMARY KEY (aggregate_id, version)
);

CREATE TABLE snapshots (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    PRIMARY KEY (aggregate_id, version)
);

-- Two Store instances, one per table:
eventStore := &SQLStore{table: "events"}
snapshotStore := &SQLStore{table: "snapshots"}

asynx.New[Order]().
    WithEventStore(eventStore).
    WithSnapshotStore(snapshotStore).
    Build()
```

Or a single Store handles both:

```sql
CREATE TABLE streams (
    stream_type  TEXT    NOT NULL,  -- "events" or "snapshots"
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    PRIMARY KEY (stream_type, aggregate_id, version)
);

-- Single Store instance:
store := &SQLStore{table: "streams"}

asynx.New[Order]().
    WithEventStore(store).
    WithSnapshotStore(store).  -- same store
    Build()
```

---

## Implementation Requirements

### Multi-Node Consistency Guarantee

The `(aggregateID, version)` uniqueness constraint is the **only** coordination mechanism in Asynx. It's simple and powerful:

1. Node A writes (order_123, version 5)
2. Node B also tries to write (order_123, version 5) simultaneously
3. One succeeds, one gets uniqueness violation error
4. The loser calls Append with the same (aggregateID, version) pair again? **No** — it must retry Send() from scratch
5. Retry loads fresh state from the store, revalidates the command, and tries again with a new version

This is fundamental: **blind retries with incremented versions corrupt the stream.**

### Serialization

The Store interface works with raw bytes. Asynx handles all serialization:

- **Events** → stored as RFC 6902 JSON patches (serialized by eventstore.writer)
- **Snapshots** → stored as full aggregate state (JSON, serialized by eventstore.writer)
- **Deserialization** → handled by eventstore.reader and replayer

The Store implementation must NOT:
- Parse or validate the bytes
- Deserialize to/from Go types
- Apply schema logic
- Decrypt or decompress (unless that's the store's own internal optimization)

### Error Semantics

The Store returns `error` for all three methods. The caller (eventstore, processor) interprets specific errors:

- Uniqueness violation on Append → `ErrPipelineFailed` (retry from scratch)
- Context cancelled → `ErrContextCancelled`
- Storage unavailable → propagate as-is (caller decides what to do)

The Store should **not** try to distinguish between different error categories — just return the error. Asynx and the application layer handle interpretation.

### Testing

Asynx ships `asynx.NewMemoryStore()` — a simple in-memory Store for testing. It's not production-safe, but it's useful for testing commands and projections without infrastructure.

Developers implementing custom Store should provide their own in-memory test doubles for unit testing.

### Context Handling

All three methods receive `ctx context.Context`. Implementations must:

- Respect context cancellation — if `ctx.Done()` fires, stop the operation and return the context error
- Respect context deadlines — if approaching deadline, return deadline exceeded error
- Not add extra timeouts on top of the context (context is the authority on timing)

Example:

```go
func (s *MyStore) Append(ctx context.Context, aggregateID string, version int64, data []byte) error {
    // Check context before starting
    select {
    case <-ctx.Done():
        return ctx.Err()
    default:
    }

    // Perform write with context awareness
    result := make(chan error, 1)
    go func() {
        result <- s.writeToDatabase(aggregateID, version, data)
    }()

    select {
    case err := <-result:
        return err
    case <-ctx.Done():
        // Context cancelled mid-operation
        return ctx.Err()
    }
}
```

---

## Example: SQL-Backed Store

A typical Store implementation for Postgres or SQLite:

```go
type SQLStore struct {
    db *sql.DB
}

func (s *SQLStore) Append(ctx context.Context, aggregateID string, version int64, data []byte) error {
    const query = `
        INSERT INTO events (aggregate_id, version, data)
        VALUES ($1, $2, $3)
    `

    _, err := s.db.ExecContext(ctx, query, aggregateID, version, data)

    // Check if error is due to uniqueness violation
    if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
        // Return some error indicating uniqueness violation
        // Asynx will interpret as ErrPipelineFailed
        return fmt.Errorf("version conflict: (%s, %d) already exists", aggregateID, version)
    }

    return err  // Include context cancellation errors, storage errors, etc.
}

func (s *SQLStore) ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error) {
    const query = `
        SELECT data FROM events
        WHERE aggregate_id = $1 AND version >= $2
        ORDER BY version ASC
    `

    rows, err := s.db.QueryContext(ctx, query, aggregateID, fromVersion)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var results [][]byte
    for rows.Next() {
        var data []byte
        if err := rows.Scan(&data); err != nil {
            return nil, err
        }
        results = append(results, data)
    }

    return results, rows.Err()
}

func (s *SQLStore) ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error) {
    const query = `
        SELECT data FROM events
        WHERE aggregate_id = $1 AND version >= $2
        ORDER BY version ASC
        LIMIT $3
    `

    rows, err := s.db.QueryContext(ctx, query, aggregateID, fromVersion, count)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var results [][]byte
    for rows.Next() {
        var data []byte
        if err := rows.Scan(&data); err != nil {
            return nil, err
        }
        results = append(results, data)
    }

    return results, rows.Err()
}
```

---

## Example: Schema

A minimal SQL schema for event and snapshot streams:

```sql
-- Events stream
CREATE TABLE events (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
);

CREATE INDEX idx_events_aggregate_id ON events(aggregate_id, version);

-- Snapshots stream (could be same table with stream_type, or separate table)
CREATE TABLE snapshots (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
);

CREATE INDEX idx_snapshots_aggregate_id ON snapshots(aggregate_id, version);
```

Or combined:

```sql
CREATE TABLE streams (
    stream_type  TEXT     NOT NULL,  -- "events" or "snapshots"
    aggregate_id TEXT     NOT NULL,
    version      INTEGER  NOT NULL,
    data         BLOB     NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_type, aggregate_id, version)
);

CREATE INDEX idx_streams ON streams(stream_type, aggregate_id, version);
```

---

## Known Limitations

**No hard deletes.** The Store is append-only — entries cannot be deleted or overwritten. For GDPR "right to be forgotten," implement crypto-shredding at the application layer: encrypt each aggregate's events with a per-aggregate key and destroy the key to render events unreadable. Key management is outside Asynx's scope.

**No ordering across aggregates.** Streams are ordered per aggregate (version order), but there is no global ordering across all aggregates. This is intentional — it avoids a distributed consensus problem. If you need cross-aggregate causal ordering, add correlation IDs or timestamps to your projections.

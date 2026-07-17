# Store Package Specification

## Overview

The `models` package defines two independent persistence contracts: `Store` — the append-only **event** stream contract — and `SnapshotStore` — a single upserted cache cell per aggregate. This document covers `Store`. See [SnapshotStore](#snapshotstore) below for the other.

`Store` accepts five operations: append a blob to a stream, read all entries from a version, read a bounded range, count entries from a version, and delete every entry for an aggregate.

The Store interface is intentionally minimal — five methods, raw bytes, no opinions about serialization or schema. Asynx owns what goes into the blobs (RFC 6902 diffs); the developer owns how and where they are stored. Developers implement Store for whatever infrastructure they have: SQLite, Postgres, Redis, DynamoDB, cloud storage, etc.

The critical responsibility is enforcing the `(aggregateID, version)` uniqueness constraint — this is the only coordination mechanism needed for multi-node consistency.

## Types & Interfaces

### `Store` Interface

The event stream persistence contract. Append-only semantics for `Append`/`ReadFrom`/`ReadRange`/`Count`; `Delete` is the sole exception, added for forget-as-a-service (see [Known Limitations](#known-limitations)).

```go
type Store interface {
    // Append writes a single entry to the named stream at the given version.
    Append(ctx context.Context, aggregateID string, version int64, data []byte) error

    // ReadFrom returns all entries in the named stream from fromVersion onward.
    ReadFrom(ctx context.Context, aggregateID string, fromVersion int64) ([][]byte, error)

    // ReadRange returns up to count entries from the stream starting at fromVersion.
    ReadRange(ctx context.Context, aggregateID string, fromVersion int64, count int64) ([][]byte, error)

    // Count returns the number of entries with version >= fromVersion.
    Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error)

    // Delete removes all records for the given aggregateID.
    // Idempotent — deleting a non-existent aggregateID is not an error.
    Delete(ctx context.Context, aggregateID string) error
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
  - Asynx always calls Store with the stream name `events:{aggregateID}` (stream naming is Asynx's concern, not the store's) — see [Stream Naming Convention](#stream-naming-convention)
- `version` — the monotonically increasing version for this aggregate (must be >= 1)
- `data` — the blob to store (an RFC 6902 diff, serialized as JSON)

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
// If the snapshot is at version 5, read starting from version 6:
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
Like `ReadFrom`, but returns at most `count` entries instead of the entire stream. Used when the caller knows exactly how many entries it needs — e.g. `Exists()` issues a `ReadRange(fromVersion=1, count=1)` to check for at least one event without loading the whole stream, and `Replay()` uses it for a specific version range.

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

### Method: `Count`

**Signature**
```go
Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error)
```

**Purpose**
Returns the number of entries at or after `fromVersion`, without transferring the entries themselves. The writer uses this to compute the next version to append at: it reads the trusted version from the latest snapshot (0 if none), then calls `Count(ctx, aggregateID, snapVersion+1)` and adds the result to get `nextVersion` — cheaper than reading and unmarshalling every delta blob just to count them.

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate to count entries for (non-empty string)
- `fromVersion` — inclusive starting version (>= 0)

**Return Values**
- `int64` — number of entries with version >= fromVersion (0 if the aggregate has no entries in that range)
- `error` — non-nil if the count failed

**Invariants**
- **Equivalent to `len(ReadFrom(ctx, aggregateID, fromVersion))`** without materializing the blobs — implementations should use a native count query (`SELECT COUNT(*) ...`) rather than reading and discarding data
- **No entries is not an error** — returns `(0, nil)`

**Side Effects**
- None — pure read operation

**Error Handling**
- Storage unavailable → return error
- Context cancelled → return context error

**Example**
```go
// SQL implementation:
func (s *SQLStore) Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error) {
    const query = `SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND version >= $2`
    var n int64
    err := s.db.QueryRowContext(ctx, query, aggregateID, fromVersion).Scan(&n)
    return n, err
}
```

---

### Method: `Delete`

**Signature**
```go
Delete(ctx context.Context, aggregateID string) error
```

**Purpose**
Removes every entry for `aggregateID` from the stream. This is the only method on `Store` that breaks append-only semantics — it exists to support forget-as-a-service (GDPR-style erasure). See [forget.md](./forget.md) and [Known Limitations](#known-limitations).

**Parameters**
- `ctx` — context for cancellation and timeouts
- `aggregateID` — the aggregate whose entries should be removed (non-empty string)

**Return Values**
- `error` — non-nil if the deletion failed

**Invariants**
- **Idempotent** — deleting a non-existent or already-deleted `aggregateID` returns `nil`, not an error
- **Total** — every version for the aggregate is removed; there is no partial or version-scoped delete
- **Called by `Asynx.Forget`, never by ordinary command processing** — no other code path in Asynx calls `Delete`

**Side Effects**
- Permanently removes durable data — this is a genuine hard delete, not a tombstone

**Error Handling**
- Storage unavailable → return error; `Forget` surfaces `ErrForgetFailed` wrapping it
- Context cancelled → return context error

**Example**
```go
// SQL implementation:
func (s *SQLStore) Delete(ctx context.Context, aggregateID string) error {
    const query = `DELETE FROM events WHERE aggregate_id = $1`
    _, err := s.db.ExecContext(ctx, query, aggregateID)
    return err
}
```

---

## Stream Naming Convention

Asynx owns stream naming. The developer's `Store` implementation receives `aggregateID` as-is for `Append`/`ReadFrom`/`ReadRange`/`Count`/`Delete` — but every call is prefixed by Asynx before it reaches the store:

```
events:{aggregateID}  → append-only stream of RFC 6902 patches
```

For example, `store.Append(ctx, "order_123", 5, patch)` never happens directly — Asynx calls `store.Append(ctx, "events:order_123", 5, patch)`. The developer's implementation just needs to treat whatever string it receives as an opaque stream key; it doesn't need to know or care that the `events:` prefix exists.

**There is only one prefix.** Before this release, snapshots were written through this same `Store` interface under a separate, similarly-prefixed stream, so a `Store` implementation might have seen more than one prefix. `SnapshotStore` (below) replaced that: snapshots no longer go through `Store` at all, so `Store` implementations only ever see `events:{aggregateID}`.

Typical SQL implementation:

```sql
CREATE TABLE events (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    PRIMARY KEY (aggregate_id, version)
);

-- Single Store instance:
eventStore := &SQLStore{table: "events"}

asynx.New[Order]().
    WithEventStore(eventStore).
    WithSnapshotStore(snapshotStore).  // a *different* interface — see below
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
- **Deserialization** → handled by eventstore.reader and replayer

The Store implementation must NOT:
- Parse or validate the bytes
- Deserialize to/from Go types
- Apply schema logic
- Decrypt or decompress (unless that's the store's own internal optimization)

### Error Semantics

The Store returns `error` for all five methods. The caller (eventstore, processor) interprets specific errors:

- Uniqueness violation on Append → `ErrPipelineFailed` (retry from scratch)
- Context cancelled → `ErrContextCancelled`
- Storage unavailable → propagate as-is (caller decides what to do)
- Deletion failure on Delete → `ErrForgetFailed` (wraps the underlying error)

The Store should **not** try to distinguish between different error categories — just return the error. Asynx and the application layer handle interpretation.

### Testing

Asynx ships `store.New()` in the `store` package — a simple in-memory `Store` for testing, with one-shot failure injection via `SetError`. It's not production-safe, but it's useful for testing commands and projections without infrastructure. The companion `store.NewSnapshots()` provides the same for `SnapshotStore`.

```go
ax, err := asynx.New[Order]().
    WithEventStore(store.New()).
    WithSnapshotStore(store.NewSnapshots()).
    Build()
```

Developers implementing a custom Store should provide their own in-memory test doubles for unit testing.

### Context Handling

All five methods receive `ctx context.Context`. Implementations must:

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

func (s *SQLStore) Count(ctx context.Context, aggregateID string, fromVersion int64) (int64, error) {
    const query = `SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND version >= $2`
    var n int64
    err := s.db.QueryRowContext(ctx, query, aggregateID, fromVersion).Scan(&n)
    return n, err
}

func (s *SQLStore) Delete(ctx context.Context, aggregateID string) error {
    const query = `DELETE FROM events WHERE aggregate_id = $1`
    _, err := s.db.ExecContext(ctx, query, aggregateID)
    return err
}
```

---

## Example: Schema

A minimal SQL schema for the event stream, alongside the snapshot table (see [SnapshotStore](#snapshotstore)):

```sql
-- Event stream: append-only, (aggregate_id, version) unique.
CREATE TABLE events (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_id, version)
);

-- Snapshots: exactly one row per aggregate. Note the primary key.
CREATE TABLE snapshots (
    aggregate_id TEXT    NOT NULL,
    version      INTEGER NOT NULL,
    data         BLOB    NOT NULL,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (aggregate_id)
);
```

The two tables are not interchangeable, and the difference is load-bearing: `events` is keyed on `(aggregate_id, version)` because every version must be retained; `snapshots` is keyed on `aggregate_id` alone because only the newest snapshot is ever useful.

---

## SnapshotStore

`models.SnapshotStore` is a **separate interface**, not a second `Store` instance. It persists exactly one snapshot per aggregate — the most recent — as a single upserted row, cell, or key.

```go
type SnapshotStore interface {
    // Put stores the snapshot for aggregateID, replacing any snapshot already
    // stored for it. Implementations MUST upsert: the primary key is
    // (aggregateID) alone, never (aggregateID, version).
    Put(ctx context.Context, aggregateID string, version int64, data []byte) error

    // Get returns the stored snapshot for aggregateID. found is false when no
    // snapshot exists — that is not an error, it is the normal state of an
    // aggregate that has never been snapshotted.
    Get(ctx context.Context, aggregateID string) (data []byte, found bool, err error)

    // Delete removes the snapshot for aggregateID.
    // Idempotent — deleting a non-existent aggregateID is not an error.
    Delete(ctx context.Context, aggregateID string) error
}
```

**Why not `Store`.** Nothing in Asynx ever reads a snapshot other than the newest one. Modelling snapshots as a `Store` stream forced "read the newest" to be emulated as "read every snapshot ever written for this aggregate and discard all but the last" — O(n) on every read *and* every write, with the table growing without bound. `SnapshotStore` makes "keep only the newest" the storage model directly: one row, upserted in place.

**`Put` must upsert.** The primary key is `aggregate_id` alone (see the schema above) — never `(aggregate_id, version)`. A `Put` implemented as a blind `INSERT` will violate the primary key on the second call for the same aggregate and break every subsequent snapshot write for it.

**Snapshots are a cache, not a source of truth.** Every snapshot can be rebuilt by replaying the aggregate's event stream from version 1. A `SnapshotStore` may therefore lose or discard data without affecting correctness — the only cost is a slower read next time. This is why `Get`'s `found == false` is a normal outcome, not an error: it just means the reader falls back to a full replay.

**The optional monotonicity guard.** `version` is the aggregate version the snapshot represents. Asynx never reads it back through this interface — it exists so an implementation can persist it as a column for observability, or guard the upsert against writing an older snapshot over a newer one:

```sql
ON CONFLICT (aggregate_id) DO UPDATE SET ...
WHERE excluded.version > snapshots.version
```

That guard is optional. Asynx tolerates last-write-wins: an older snapshot overwriting a newer one is safe, because the reader replays whatever delta events exist after the stored version. It only costs extra replay, never incorrect state.

**Reference implementation:**

```go
func (s *SQLSnapshotStore) Put(ctx context.Context, aggregateID string, version int64, data []byte) error {
    const query = `
        INSERT INTO snapshots (aggregate_id, version, data)
        VALUES ($1, $2, $3)
        ON CONFLICT (aggregate_id) DO UPDATE
            SET version = excluded.version, data = excluded.data
            WHERE excluded.version > snapshots.version
    `
    _, err := s.db.ExecContext(ctx, query, aggregateID, version, data)
    return err
}

func (s *SQLSnapshotStore) Get(ctx context.Context, aggregateID string) ([]byte, bool, error) {
    const query = `SELECT data FROM snapshots WHERE aggregate_id = $1`

    var data []byte
    err := s.db.QueryRowContext(ctx, query, aggregateID).Scan(&data)
    if errors.Is(err, sql.ErrNoRows) {
        return nil, false, nil
    }
    if err != nil {
        return nil, false, err
    }
    return data, true, nil
}

func (s *SQLSnapshotStore) Delete(ctx context.Context, aggregateID string) error {
    const query = `DELETE FROM snapshots WHERE aggregate_id = $1`
    _, err := s.db.ExecContext(ctx, query, aggregateID)
    return err
}
```

`WithSnapshotStore` is required on the builder — `Build()` returns `models.ErrMissingSnapshotStore` if it is nil. It cannot default to the event store: `models.Store` is not a `models.SnapshotStore`, and a `Store` implementation would need an entirely different upsert-shaped table to satisfy it anyway. See the [in-memory reference implementation](../../store/snapshot_memory.go) (`store.NewSnapshots()`) for a minimal, non-durable example.

---

## Migrating from v0.7.x

Before v0.8.0, snapshots were written to a `models.Store` as an append-only
stream: one row per snapshot, `PRIMARY KEY (aggregate_id, version)`. Reading
the current state loaded *every* snapshot ever written for the aggregate and
discarded all but the newest, so both reads and writes were O(number of
snapshots) and the table grew without bound.

Snapshots are a derived cache — every one can be rebuilt by replaying events.
So there is no data to migrate:

1. Implement `models.SnapshotStore` against a table keyed by `aggregate_id`
   alone (schema above).
2. Drop the old snapshot table, or leave it orphaned and drop it later.
3. Pass the new store to `WithSnapshotStore`. It is now required — `Build()`
   returns `ErrMissingSnapshotStore` without it, and it no longer defaults to
   the event store.

Dropping the table is safe purely because snapshots are derived from events:
`Get` cold-replays an aggregate's full event stream whenever no snapshot row
is found, and cold replay always produces correct state.

Reading does not, by itself, repair the snapshot table, though. `Get` writes
an auto-snapshot only as a side effect of upcasting — when an event's
`SchemaVersion` is older than the current one — not merely because it took
the cold path. For an aggregate with no pending schema upcast, `Get` keeps
cold-replaying on every call until something else writes a snapshot.

The snapshot row is (re)written the next time either of these happens:

- A **command** runs against the aggregate whose `ShouldSnapshot()` returns
  `true` (`EventStore.Write` writes a snapshot after appending the event).
- A **`Get`** replays at least one event that needs upcasting to the current
  schema version.

So the practical impact depends on how often each aggregate's commands set
`ShouldSnapshot()` to `true`. An aggregate that snapshots on every command (or
frequently) regains its snapshot on the very next write, with at most one
extra cold replay in between. One that snapshots rarely (or never sets
`ShouldSnapshot() == true`) will keep paying the cold-replay cost on every
`Get` until it does. Correctness is unaffected either way — only read cost.

---

## Known Limitations

**`Delete` is total and immediate, not a soft delete.** `Store.Delete` removes every version of an aggregate's event stream in one call, backing the `Forget` API for GDPR-style erasure (see [forget.md](./forget.md)). It is intentionally the only non-append-only operation on `Store` — ordinary command processing never calls it. `Delete` only reaches storage your `Store` implementation directly controls, though: replicas, backups, and WAL archives that already copied the data before `Forget` ran are outside Asynx's reach. For infrastructure where those copies can't be reliably purged, layer crypto-shredding underneath `Delete`: encrypt each aggregate's events with a per-aggregate key and destroy the key so that any surviving copy is unreadable. Key management is outside Asynx's scope.

**No ordering across aggregates.** Streams are ordered per aggregate (version order), but there is no global ordering across all aggregates. This is intentional — it avoids a distributed consensus problem. If you need cross-aggregate causal ordering, add correlation IDs or timestamps to your projections.

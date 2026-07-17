package models

import "context"

// SnapshotStore persists exactly one snapshot per aggregate: the most recent.
//
// Snapshots are a derived cache, never a source of truth — every snapshot can
// be rebuilt by replaying the aggregate's event stream from version 1. A
// SnapshotStore may therefore lose or discard data without affecting
// correctness; the only cost is a slower read.
//
// This is deliberately not a Store. Snapshots are a single mutable cell keyed
// by aggregateID, not an append-only versioned stream: nothing in asynx ever
// reads a snapshot other than the newest one. Modelling them as a Store stream
// forced "read the newest" to be emulated as "read every snapshot ever written
// and discard all but the last", which is O(n) on every read and every write.
type SnapshotStore interface {
	// Put stores the snapshot for aggregateID, replacing any snapshot already
	// stored for it. Implementations MUST upsert: the primary key is
	// (aggregateID) alone, never (aggregateID, version).
	//
	// version is the aggregate version this snapshot represents. Asynx never
	// reads it back through this interface — it is passed so implementations
	// can persist it as a column for observability, or guard the upsert with a
	// monotonicity check such as:
	//
	//	ON CONFLICT (aggregate_id) DO UPDATE SET ...
	//	WHERE excluded.version > snapshots.version
	//
	// That guard is optional. Asynx tolerates last-write-wins: an older
	// snapshot overwriting a newer one is safe, because the reader replays
	// delta events from the stored version onward. It only costs extra replay.
	Put(
		ctx context.Context,
		aggregateID string,
		version int64,
		data []byte,
	) error

	// Get returns the stored snapshot for aggregateID.
	//
	// found is false when no snapshot exists. That is not an error — it is the
	// normal state of an aggregate that has never been snapshotted, and callers
	// respond by replaying from version 1.
	Get(
		ctx context.Context,
		aggregateID string,
	) (data []byte, found bool, err error)

	// Delete removes the snapshot for aggregateID.
	// Idempotent — deleting a non-existent aggregateID is not an error.
	Delete(
		ctx context.Context,
		aggregateID string,
	) error
}

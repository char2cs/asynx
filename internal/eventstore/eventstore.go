package eventstore

import (
	"github.com/char2cs/asynx/internal/eventstore/internal/core"
	"github.com/char2cs/asynx/internal/eventstore/internal/ports"
	asynxmd "github.com/char2cs/asynx/models"
)

// EventStore is the durable store of aggregate state: every command appends one
// event to the aggregate's stream and state is rebuilt from the latest snapshot
// plus the events after it. The interface is the union of the read side
// (Reading), the write side (Writing) and the administrative side
// (Maintenance); see those for the per-method contracts. An EventStore is safe
// for concurrent use, but it does not serialise commands for one aggregate:
// concurrent appends at the same version conflict and one fails. Construct it
// with New.
type EventStore[T any] interface {
	ports.Reading[T]
	ports.Writing[T]
	ports.Maintenance[T]
}

// New builds an EventStore over two stores with different contracts, which
// therefore cannot be the same instance: eventStore holds the events and their
// transactional extras, snapshotStore holds the one latest snapshot per
// aggregate. upcasters maps a schema version to the migration that lifts an
// event from that version to the next; schemaVersion is the current schema
// version stamped on new events and the target of every upcast. onCorruption is
// called with the decoding error when a stored snapshot cannot be read, and the
// store then rebuilds state from events; pass nil to rebuild silently. The
// callback may run on any goroutine that reads.
func New[T any](
	eventStore asynxmd.Store,
	snapshotStore asynxmd.SnapshotStore,
	upcasters map[int]asynxmd.Upcaster,
	schemaVersion int,
	onCorruption func(error),
) EventStore[T] {
	return core.New[T](
		eventStore,
		snapshotStore,
		upcasters,
		schemaVersion,
		onCorruption,
	)
}

package eventstore

import (
	"github.com/char2cs/asynx/internal/eventstore/internal/core"
	"github.com/char2cs/asynx/internal/eventstore/internal/ports"
	asynxmd "github.com/char2cs/asynx/models"
)

// EventStore is the API for durable aggregate persistence. Construct it with
// New.
type EventStore[T any] interface {
	ports.Reading[T]
	ports.Writing[T]
	ports.Maintenance[T]
}

// New builds a fully-configured EventStore. eventStore and snapshotStore use
// different contracts and cannot be the same instance. upcasters maps a schema
// version to its migration; schemaVersion is the current schema version for new
// events. onCorruption is called when a snapshot cannot be deserialized; pass
// nil to fall back to the cold replay path silently.
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

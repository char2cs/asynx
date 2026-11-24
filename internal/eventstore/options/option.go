package options

import (
	"context"

	"github.com/char2cs/asynx/internal/eventstore/internal/core"
	asynxmd "github.com/char2cs/asynx/models"
)

// Option configures an EventStore at construction; pass options to eventstore.New.
type Option[T any] func(
	*core.EventStore[T],
)

// WithSnapshotErrorHandler makes the store report a failed auto-snapshot write
// to fn. When a read (Get, Load, Preload) had to upcast old events it caches
// the upcast state as a snapshot, best effort: a failed write never fails the
// read, because the state is already durable in the event store and a missing
// snapshot only costs a slower next read. Without this option, or with a nil
// fn, that failure is dropped.
//
// fn runs synchronously on the goroutine that performed the read, after the
// state was computed and before the read returns; for a lock-free Get it runs
// while the aggregate's command lock is held, so it should return quickly and
// must not call back into the aggregate's commands. A panic in fn is contained
// and never breaks the read. The Event fn receives carries only what a read
// knows: AggregateID, Version (the version the snapshot was meant for), ID (the
// id of that version's event), SchemaVersion (the current schema version) and
// Aggregate (the state that was not cached); EventName, OccurredAt and
// PreviousAggregate are zero.
func WithSnapshotErrorHandler[T any](
	fn func(context.Context, asynxmd.Event[T], error),
) Option[T] {
	return func(
		es *core.EventStore[T],
	) {
		es.ObserveAutoSnapshotErrors(fn)
	}
}

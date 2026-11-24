package ports

import (
	"context"

	esmodels "github.com/char2cs/asynx/v2/internal/eventstore/models"
	"github.com/char2cs/asynx/v2/models/command"
	"github.com/char2cs/asynx/v2/models/event"
)

// Writing is the write side of an event store: it appends one event per command
// and stores snapshots. Appends are optimistic: the event is written at the
// version the command was validated against, so two commands validated against
// the same state cannot both commit. Serialising commands per aggregate is the
// caller's job (the command processor does it); the store only detects the
// conflict.
type Writing[T any] interface {
	// Write loads the aggregate, validates cmd against its current state,
	// appends the event cmd emits and, when cmd asks for a snapshot, stores one.
	// It returns the committed event. A validation failure returns the
	// command's error, a lost version race returns models.ErrPipelineFailed
	// semantics (re-read and retry) and a failed snapshot write is returned even
	// though the event is already durable. The command processor does not use
	// Write: it composes WriteTx and Snapshot to treat snapshot failure as
	// non-fatal.
	Write(
		ctx context.Context,
		cmd command.Command[T],
	) (event.Event[T], error)
	// WriteTx is Write without the snapshot, with hook run inside the append's
	// transaction right after the event is appended. When hook returns an
	// error the transaction rolls back and no event is stored. hook may be nil.
	WriteTx(
		ctx context.Context,
		cmd command.Command[T],
		hook esmodels.TxHook[T],
	) (event.Event[T], error)
	// Snapshot stores the state evt produced as the aggregate's snapshot,
	// replacing any earlier one. It is idempotent and safe to retry; a failure
	// loses nothing because snapshots can always be rebuilt from events.
	Snapshot(
		ctx context.Context,
		evt event.Event[T],
	) error
}

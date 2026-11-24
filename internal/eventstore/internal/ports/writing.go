package ports

import (
	"context"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
)

// Writing appends events and snapshots.
type Writing[T any] interface {
	// Write validates cmd, appends its event and snapshots when due.
	Write(
		ctx context.Context,
		cmd asynxmd.Command[T],
	) (asynxmd.Event[T], error)
	// WriteTx is Write with hook running inside the append's transaction.
	WriteTx(
		ctx context.Context,
		cmd asynxmd.Command[T],
		hook esmodels.TxHook[T],
	) (asynxmd.Event[T], error)
	// Snapshot writes the snapshot of the state evt produced.
	Snapshot(
		ctx context.Context,
		evt asynxmd.Event[T],
	) error
}

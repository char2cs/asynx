package exec

import (
	"context"
	"fmt"

	"github.com/char2cs/asynx/v2/models/dispatch"
	"github.com/char2cs/asynx/v2/models/failure"
)

type tombstoneWrite[T any] struct {
	executor *CommandExecutor[T]
	ticket   dispatch.Ticket
}

func (w *tombstoneWrite[T]) commit(
	ctx context.Context,
	aggregateID string,
) error {
	e := w.executor
	err := e.locks.Lock(
		ctx,
		aggregateID,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			failure.ErrContextCancelled,
			err,
		)
	}

	defer e.locks.Unlock(aggregateID)

	if err := e.refuseIfForgetting(aggregateID); err != nil {
		return err
	}

	e.markForgetting(aggregateID)

	w.ticket, err = e.commitTombstone(
		ctx,
		aggregateID,
	)
	if err != nil {
		e.clearForgetting(aggregateID)
		w.ticket = dispatch.Ticket{}
	}

	return err
}

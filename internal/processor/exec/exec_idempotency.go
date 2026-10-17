package exec

import (
	"context"
	"errors"
	"fmt"

	asynxmd "github.com/char2cs/asynx/models"
)

// writeFailed classifies a WriteTx error. handled is true when the outcome is
// final for the caller (a retry answer); otherwise err is the error to return.
func (e *CommandExecutor[T]) writeFailed(
	ctx context.Context,
	commandID string,
	err error,
) (event asynxmd.Event[T], handled bool, _ error) {
	switch {
	case errors.Is(err, asynxmd.ErrSyncProjection):
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)

	case commandID != "" && errors.Is(err, asynxmd.ErrDuplicateReceipt):
		// Another aggregate already used this command id: answer from its receipt.
		if event, handled, err := e.answerFromReceipt(ctx, commandID); handled {
			return event, true, err
		}
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)

	case errors.Is(err, asynxmd.ErrValidation):
		if commandID == "" {
			return event, false, err
		}
		// The rejection must be durable before the caller hears about it, or a
		// retry of the same id could pass validation later and succeed.
		recErr := e.recordRejection(ctx, commandID, err)
		switch {
		case recErr == nil:
			return event, false, err
		case errors.Is(recErr, asynxmd.ErrDuplicateReceipt):
			// Someone already decided this id: answer from their receipt.
			if event, handled, aerr := e.answerFromReceipt(ctx, commandID); handled {
				return event, true, aerr
			}
		}
		return event, false, fmt.Errorf("%w: recording rejection: %w", asynxmd.ErrPipelineFailed, recErr)

	default:
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
}

// recordRejection stores a rejected receipt so a retry of the same command id
// stays rejected. Failing to store it fails the command with ErrPipelineFailed
// instead of reporting a rejection that could later be overturned; the caller
// retries and re-validates.
func (e *CommandExecutor[T]) recordRejection(ctx context.Context, commandID string, cause error) error {
	return e.es.Store().Atomically(ctx, func(tx asynxmd.Tx) error {
		return tx.PutReceipt(ctx, commandID, asynxmd.Receipt{Rejection: cause.Error()})
	})
}

// answerFromReceipt answers a retry of an already-decided command.
func (e *CommandExecutor[T]) answerFromReceipt(
	ctx context.Context,
	commandID string,
) (event asynxmd.Event[T], handled bool, err error) {
	receipt, found, err := e.es.Store().GetReceipt(ctx, commandID)
	if err != nil {
		return event, true, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
	if !found {
		return event, false, nil
	}
	if !receipt.Accepted {
		return event, true, fmt.Errorf("%w: %s", asynxmd.ErrValidation, receipt.Rejection)
	}

	event = asynxmd.Event[T]{
		ID:          receipt.EventID,
		AggregateID: receipt.AggregateID,
		Version:     receipt.Version,
	}
	// Rebuild the full event when its stream still exists.
	_ = e.es.Replay(ctx, receipt.AggregateID, receipt.Version, receipt.Version,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if evt.Version == receipt.Version {
				event = evt
			}
		})
	return event, true, nil
}

func idempotencyKey[T any](cmd asynxmd.Command[T]) string {
	if ic, ok := cmd.(asynxmd.IdempotentCommand); ok {
		return ic.CommandID()
	}
	return ""
}

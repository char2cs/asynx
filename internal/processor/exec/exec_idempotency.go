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
		if event, handled, err := e.answerFromReceipt(ctx, commandID); handled {
			return event, true, err
		}
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)

	case errors.Is(err, asynxmd.ErrValidation):
		return e.validationFailed(ctx, commandID, err)

	default:
		return event, false, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}
}

// validationFailed makes a rejection final before it is reported: it is
// recorded, or answered from the receipt someone else recorded first.
func (e *CommandExecutor[T]) validationFailed(
	ctx context.Context,
	commandID string,
	rejection error,
) (event asynxmd.Event[T], handled bool, err error) {
	if commandID == "" {
		return event, false, rejection
	}
	recErr := e.recordRejection(ctx, commandID, rejection)
	if recErr == nil {
		return event, false, rejection
	}
	lostRaceToAnotherDecision := errors.Is(recErr, asynxmd.ErrDuplicateReceipt)
	if lostRaceToAnotherDecision {
		if event, handled, aerr := e.answerFromReceipt(ctx, commandID); handled {
			return event, true, aerr
		}
	}
	return event, false, fmt.Errorf("%w: recording rejection: %w", asynxmd.ErrPipelineFailed, recErr)
}

// recordRejection stores a rejected receipt so a retry of the same command id
// stays rejected. Failing to store it fails the command with ErrPipelineFailed
// instead of reporting a rejection that could later be overturned; the caller
// retries and re-validates.
func (e *CommandExecutor[T]) recordRejection(
	ctx context.Context,
	commandID string,
	cause error,
) error {
	return e.es.Store().Atomically(ctx, func(tx asynxmd.Tx) error {
		return tx.PutReceipt(ctx, commandID, asynxmd.Receipt{Rejection: cause.Error()})
	})
}

// answerFromReceipt answers a retry of an already-decided command. The stored
// event is matched by ID as well as version, so an aggregate re-created after a
// Forget, whose versions coincide, is never mistaken for the original.
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
	replayErr := e.es.Replay(ctx, receipt.AggregateID, receipt.Version, receipt.Version,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if evt.Version == receipt.Version && evt.ID == receipt.EventID {
				event = evt
			}
		})
	if replayErr != nil {
		return event, true, fmt.Errorf("%w: reading the event of command %s: %w", asynxmd.ErrPipelineFailed, commandID, replayErr)
	}
	return event, true, nil
}

func idempotencyKey[T any](cmd asynxmd.Command[T]) string {
	if ic, ok := cmd.(asynxmd.IdempotentCommand); ok {
		return ic.CommandID()
	}
	return ""
}

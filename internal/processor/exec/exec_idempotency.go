package exec

import (
	"context"
	"errors"
	"fmt"

	asynxmd "github.com/char2cs/asynx/models"
)

func (e *CommandExecutor[T]) writeFailed(
	ctx context.Context,
	commandID string,
	err error,
) (event asynxmd.Event[T], handled bool, _ error) {
	switch {
	case errors.Is(
		err,
		asynxmd.ErrSyncProjection,
	):
		return event,
			false,
			fmt.Errorf(
				"%w: %w",
				asynxmd.ErrPipelineFailed,
				err,
			)

	case commandID != "" && errors.Is(
		err,
		asynxmd.ErrDuplicateReceipt,
	):
		if event, handled, err := e.answerFromReceipt(
			ctx,
			commandID,
		); handled {
			return event,
				true,
				err
		}

		return event,
			false,
			fmt.Errorf(
				"%w: %w",
				asynxmd.ErrPipelineFailed,
				err,
			)

	case errors.Is(
		err,
		asynxmd.ErrValidation,
	):
		return e.validationFailed(
			ctx,
			commandID,
			err,
		)

	default:
		return event,
			false,
			fmt.Errorf(
				"%w: %w",
				asynxmd.ErrPipelineFailed,
				err,
			)
	}
}

func (e *CommandExecutor[T]) validationFailed(
	ctx context.Context,
	commandID string,
	rejection error,
) (event asynxmd.Event[T], handled bool, err error) {
	if commandID == "" {
		return event,
			false,
			rejection
	}

	recErr := e.recordRejection(
		ctx,
		commandID,
		rejection,
	)
	if recErr == nil {
		return event,
			false,
			rejection
	}

	lostRaceToAnotherDecision := errors.Is(
		recErr,
		asynxmd.ErrDuplicateReceipt,
	)
	if lostRaceToAnotherDecision {
		if event, handled, aerr := e.answerFromReceipt(
			ctx,
			commandID,
		); handled {
			return event,
				true,
				aerr
		}
	}

	return event,
		false,
		fmt.Errorf(
			"%w: recording rejection: %w",
			asynxmd.ErrPipelineFailed,
			recErr,
		)
}

func (e *CommandExecutor[T]) recordRejection(
	ctx context.Context,
	commandID string,
	cause error,
) error {
	return e.es.Store().Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			return tx.PutReceipt(
				ctx,
				commandID,
				asynxmd.Receipt{Rejection: cause.Error()},
			)
		},
	)
}

func (e *CommandExecutor[T]) answerFromReceipt(
	ctx context.Context,
	commandID string,
) (event asynxmd.Event[T], handled bool, err error) {
	receipt, found, err := e.es.Store().GetReceipt(
		ctx,
		commandID,
	)
	if err != nil {
		return event,
			true,
			fmt.Errorf(
				"%w: %w",
				asynxmd.ErrPipelineFailed,
				err,
			)
	}

	if !found {
		return event,
			false,
			nil
	}

	if !receipt.Accepted {
		return event,
			true,
			fmt.Errorf(
				"%w: %s",
				asynxmd.ErrValidation,
				receipt.Rejection,
			)
	}

	event = asynxmd.Event[T]{
		ID:          receipt.EventID,
		AggregateID: receipt.AggregateID,
		Version:     receipt.Version,
	}
	replayErr := e.es.Replay(
		ctx,
		receipt.AggregateID,
		receipt.Version,
		receipt.Version,
		func(
			_ context.Context,
			evt asynxmd.Event[T],
		) {
			if evt.Version == receipt.Version &&
				evt.ID == receipt.EventID {
				event = evt
			}
		},
	)
	if replayErr != nil {
		return event,
			true,
			fmt.Errorf(
				"%w: reading the event of command %s: %w",
				asynxmd.ErrPipelineFailed,
				commandID,
				replayErr,
			)
	}

	return event,
		true,
		nil
}

func idempotencyKey[T any](
	cmd asynxmd.Command[T],
) string {
	if ic, ok := cmd.(asynxmd.IdempotentCommand); ok {
		return ic.CommandID()
	}

	return ""
}

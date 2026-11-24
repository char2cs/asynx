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
	if errors.Is(
		err,
		asynxmd.ErrSyncProjection,
	) {
		return event,
			false,
			pipelineFailed(err)
	}

	if commandID != "" &&
		errors.Is(
			err,
			asynxmd.ErrDuplicateReceipt,
		) {
		return e.answerDuplicate(
			ctx,
			commandID,
			err,
		)
	}

	if errors.Is(
		err,
		asynxmd.ErrValidation,
	) {
		return e.validationFailed(
			ctx,
			commandID,
			err,
		)
	}

	return event,
		false,
		pipelineFailed(err)
}

func pipelineFailed(
	err error,
) error {
	return fmt.Errorf(
		"%w: %w",
		asynxmd.ErrPipelineFailed,
		err,
	)
}

func (e *CommandExecutor[T]) answerDuplicate(
	ctx context.Context,
	commandID string,
	duplicate error,
) (event asynxmd.Event[T], handled bool, err error) {
	event, handled, err = e.answerFromReceipt(
		ctx,
		commandID,
	)
	if handled {
		return event,
			true,
			err
	}

	return event,
		false,
		pipelineFailed(duplicate)
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

	if errors.Is(
		recErr,
		asynxmd.ErrDuplicateReceipt,
	) {
		return e.answerLostRace(
			ctx,
			commandID,
			recErr,
		)
	}

	return event,
		false,
		recordingFailed(recErr)
}

func recordingFailed(
	recErr error,
) error {
	return fmt.Errorf(
		"%w: recording rejection: %w",
		asynxmd.ErrPipelineFailed,
		recErr,
	)
}

func (e *CommandExecutor[T]) answerLostRace(
	ctx context.Context,
	commandID string,
	recErr error,
) (event asynxmd.Event[T], handled bool, err error) {
	event, handled, err = e.answerFromReceipt(
		ctx,
		commandID,
	)
	if handled {
		return event,
			true,
			err
	}

	return event,
		false,
		recordingFailed(recErr)
}

func (e *CommandExecutor[T]) recordRejection(
	ctx context.Context,
	commandID string,
	cause error,
) error {
	return e.es.Store().Atomically(
		ctx,
		recordingRejection(
			ctx,
			commandID,
			cause,
		),
	)
}

func recordingRejection(
	ctx context.Context,
	commandID string,
	cause error,
) func(asynxmd.Tx) error {
	return func(
		tx asynxmd.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			commandID,
			asynxmd.Receipt{Rejection: cause.Error()},
		)
	}
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
			pipelineFailed(err)
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

	event, err = e.replayReceipt(
		ctx,
		receipt,
	)
	return event,
		true,
		receiptReadFailed(
			commandID,
			err,
		)
}

func receiptReadFailed(
	commandID string,
	err error,
) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(
		"%w: reading the event of command %s: %w",
		asynxmd.ErrPipelineFailed,
		commandID,
		err,
	)
}

func pickReceiptEvent[T any](
	current asynxmd.Event[T],
	evt asynxmd.Event[T],
	receipt asynxmd.Receipt,
) asynxmd.Event[T] {
	if evt.Version == receipt.Version &&
		evt.ID == receipt.EventID {
		return evt
	}

	return current
}

func (e *CommandExecutor[T]) replayReceipt(
	ctx context.Context,
	receipt asynxmd.Receipt,
) (event asynxmd.Event[T], err error) {
	event = asynxmd.Event[T]{
		ID:          receipt.EventID,
		AggregateID: receipt.AggregateID,
		Version:     receipt.Version,
	}
	err = e.es.Replay(
		ctx,
		receipt.AggregateID,
		receipt.Version,
		receipt.Version,
		capturingReceiptEvent(
			&event,
			receipt,
		),
	)
	return event,
		err
}

func capturingReceiptEvent[T any](
	event *asynxmd.Event[T],
	receipt asynxmd.Receipt,
) asynxmd.ProjectionHandler[T] {
	return func(
		_ context.Context,
		evt asynxmd.Event[T],
	) {
		*event = pickReceiptEvent(
			*event,
			evt,
			receipt,
		)
	}
}

func idempotencyKey[T any](
	cmd asynxmd.Command[T],
) string {
	if ic, ok := cmd.(asynxmd.IdempotentCommand); ok {
		return ic.CommandID()
	}

	return ""
}

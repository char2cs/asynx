package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/char2cs/asynx/models/command"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/storage"
)

func (e *CommandExecutor[T]) writeFailed(
	ctx context.Context,
	commandID string,
	err error,
) (evt event.Event[T], handled bool, _ error) {
	if errors.Is(
		err,
		failure.ErrSyncProjection,
	) {
		return evt,
			false,
			pipelineFailed(err)
	}

	if commandID != "" &&
		errors.Is(
			err,
			failure.ErrDuplicateReceipt,
		) {
		return e.answerDuplicate(
			ctx,
			commandID,
			err,
		)
	}

	if errors.Is(
		err,
		failure.ErrValidation,
	) {
		return e.validationFailed(
			ctx,
			commandID,
			err,
		)
	}

	return evt,
		false,
		pipelineFailed(err)
}

func pipelineFailed(
	err error,
) error {
	return fmt.Errorf(
		"%w: %w",
		failure.ErrPipelineFailed,
		err,
	)
}

func (e *CommandExecutor[T]) answerDuplicate(
	ctx context.Context,
	commandID string,
	duplicate error,
) (evt event.Event[T], handled bool, err error) {
	evt, handled, err = e.answerFromReceipt(
		ctx,
		commandID,
	)
	if handled {
		return evt,
			true,
			err
	}

	return evt,
		false,
		pipelineFailed(duplicate)
}

func (e *CommandExecutor[T]) validationFailed(
	ctx context.Context,
	commandID string,
	rejection error,
) (evt event.Event[T], handled bool, err error) {
	if commandID == "" {
		return evt,
			false,
			rejection
	}

	recErr := e.recordRejection(
		ctx,
		commandID,
		rejection,
	)
	if recErr == nil {
		return evt,
			false,
			rejection
	}

	if errors.Is(
		recErr,
		failure.ErrDuplicateReceipt,
	) {
		return e.answerLostRace(
			ctx,
			commandID,
			recErr,
		)
	}

	return evt,
		false,
		recordingFailed(recErr)
}

func recordingFailed(
	recErr error,
) error {
	return fmt.Errorf(
		"%w: recording rejection: %w",
		failure.ErrPipelineFailed,
		recErr,
	)
}

func (e *CommandExecutor[T]) answerLostRace(
	ctx context.Context,
	commandID string,
	recErr error,
) (evt event.Event[T], handled bool, err error) {
	evt, handled, err = e.answerFromReceipt(
		ctx,
		commandID,
	)
	if handled {
		return evt,
			true,
			err
	}

	return evt,
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
) func(storage.Tx) error {
	return func(
		tx storage.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			commandID,
			event.Receipt{Rejection: cause.Error()},
		)
	}
}

func (e *CommandExecutor[T]) answerFromReceipt(
	ctx context.Context,
	commandID string,
) (evt event.Event[T], handled bool, err error) {
	receipt, found, err := e.es.Store().GetReceipt(
		ctx,
		commandID,
	)
	if err != nil {
		return evt,
			true,
			pipelineFailed(err)
	}

	if !found {
		return evt,
			false,
			nil
	}

	if !receipt.Accepted {
		return evt,
			true,
			fmt.Errorf(
				"%w: %s",
				failure.ErrValidation,
				receipt.Rejection,
			)
	}

	evt, err = e.replayReceipt(
		ctx,
		receipt,
	)
	return evt,
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
		failure.ErrPipelineFailed,
		commandID,
		err,
	)
}

func pickReceiptEvent[T any](
	current event.Event[T],
	evt event.Event[T],
	receipt event.Receipt,
) event.Event[T] {
	if evt.Version == receipt.Version &&
		evt.ID == receipt.EventID {
		return evt
	}

	return current
}

func (e *CommandExecutor[T]) replayReceipt(
	ctx context.Context,
	receipt event.Receipt,
) (evt event.Event[T], err error) {
	evt = event.Event[T]{
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
			&evt,
			receipt,
		),
	)
	return evt,
		err
}

func capturingReceiptEvent[T any](
	captured *event.Event[T],
	receipt event.Receipt,
) handler.Projection[T] {
	return func(
		_ context.Context,
		evt event.Event[T],
	) {
		*captured = pickReceiptEvent(
			*captured,
			evt,
			receipt,
		)
	}
}

func idempotencyKey[T any](
	cmd command.Command[T],
) string {
	if ic, ok := cmd.(command.Idempotent); ok {
		return ic.CommandID()
	}

	return ""
}

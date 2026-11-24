package exec

import (
	"context"
	"fmt"
	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/internal/firsterror"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/storage"
)

func (e *CommandExecutor[T]) runSyncProjections(
	ctx context.Context,
	tx storage.Tx,
	evt event.Event[T],
) error {
	err := firsterror.Of(
		e.syncs,
		projecting(
			ctx,
			tx,
			evt,
		),
	)
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			failure.ErrSyncProjection,
			err,
		)
	}

	return nil
}

func projecting[T any](
	ctx context.Context,
	tx storage.Tx,
	evt event.Event[T],
) func(models.SyncProjection[T]) error {
	return func(
		p models.SyncProjection[T],
	) error {
		return runProjection(
			ctx,
			tx,
			p,
			evt,
		)
	}
}

func runProjection[T any](
	ctx context.Context,
	tx storage.Tx,
	p models.SyncProjection[T],
	evt event.Event[T],
) error {
	if !p.Match(evt.EventName) {
		return nil
	}

	return p.Fn(
		ctx,
		tx,
		evt,
	)
}

func (e *CommandExecutor[T]) txHook(
	commandID string,
) esmodels.TxHook[T] {
	if len(e.syncs) == 0 &&
		e.durableMatch == nil &&
		commandID == "" {
		return nil
	}

	return func(
		ctx context.Context,
		tx storage.Tx,
		evt event.Event[T],
	) error {
		return e.commitHook(
			ctx,
			tx,
			commandID,
			evt,
		)
	}
}

func (e *CommandExecutor[T]) commitHook(
	ctx context.Context,
	tx storage.Tx,
	commandID string,
	evt event.Event[T],
) error {
	if err := e.runSyncProjections(
		ctx,
		tx,
		evt,
	); err != nil {
		return err
	}

	if err := putAcceptedReceipt(
		ctx,
		tx,
		commandID,
		evt,
	); err != nil {
		return err
	}

	if e.isDurable(evt.EventName) {
		return tx.EnqueueDelivery(
			ctx,
			evt.AggregateID,
			evt.Version,
		)
	}

	return nil
}

func putAcceptedReceipt[T any](
	ctx context.Context,
	tx storage.Tx,
	commandID string,
	evt event.Event[T],
) error {
	if commandID == "" {
		return nil
	}

	receipt := event.Receipt{
		Accepted:    true,
		AggregateID: evt.AggregateID,
		EventID:     evt.ID,
		Version:     evt.Version,
	}
	return tx.PutReceipt(
		ctx,
		commandID,
		receipt,
	)
}

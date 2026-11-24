package core

import (
	"context"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	asynxmd "github.com/char2cs/asynx/models"
)

type pendingAppend[T any] struct {
	store         *EventStore[T]
	cmd           asynxmd.Command[T]
	hook          esmodels.TxHook[T]
	loadedVersion int64
	previousState T
	newState      T
	evt           asynxmd.Event[T]
}

func (p *pendingAppend[T]) inTransaction(
	ctx context.Context,
) func(asynxmd.Tx) error {
	return func(
		tx asynxmd.Tx,
	) error {
		return p.appendIn(
			ctx,
			tx,
		)
	}
}

func (p *pendingAppend[T]) appendIn(
	ctx context.Context,
	tx asynxmd.Tx,
) error {
	evt, err := p.store.appendWithHook(
		ctx,
		tx,
		p.cmd,
		p.hook,
		p.loadedVersion,
		p.previousState,
		p.newState,
	)
	p.evt = evt
	return err
}

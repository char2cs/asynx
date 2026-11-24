package core

import (
	"context"

	esmodels "github.com/char2cs/asynx/internal/eventstore/models"
	"github.com/char2cs/asynx/models/command"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/storage"
)

type pendingAppend[T any] struct {
	store         *EventStore[T]
	cmd           command.Command[T]
	hook          esmodels.TxHook[T]
	loadedVersion int64
	previousState T
	newState      T
	evt           event.Event[T]
}

func (p *pendingAppend[T]) inTransaction(
	ctx context.Context,
) func(storage.Tx) error {
	return func(
		tx storage.Tx,
	) error {
		return p.appendIn(
			ctx,
			tx,
		)
	}
}

func (p *pendingAppend[T]) appendIn(
	ctx context.Context,
	tx storage.Tx,
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

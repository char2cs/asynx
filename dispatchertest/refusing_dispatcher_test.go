package dispatchertest

import (
	"context"
	"errors"

	"github.com/char2cs/asynx/models"
)

var errRefused = errors.New("dispatchertest: refused")

type refusingDispatcher struct {
	hollowDispatcher
}

func refusing(
	models.DispatcherConfig[Aggregate],
) (models.Dispatcher[Aggregate], error) {
	return refusingDispatcher{}, nil
}

func (refusingDispatcher) Enqueue(
	context.Context,
	models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	return models.Ticket{}, errRefused
}

func (refusingDispatcher) Close(
	context.Context,
) error {
	return errRefused
}

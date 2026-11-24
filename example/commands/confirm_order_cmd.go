package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ConfirmOrderCmd confirms a pending order.
type ConfirmOrderCmd struct {
	ID string
}

func (c ConfirmOrderCmd) AggregateID() string { return c.ID }

func (c ConfirmOrderCmd) EventName() string { return "OrderConfirmed" }

func (c ConfirmOrderCmd) ShouldSnapshot() bool { return false }

func (c ConfirmOrderCmd) Validate(
	current *domain.Order,
) error {
	if current == nil {
		return models.ErrValidation
	}

	if current.Status != domain.OrderPending {
		return models.ErrValidation
	}

	return nil
}

func (c ConfirmOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderConfirmed
	return order
}

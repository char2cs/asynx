package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ShipOrderCmd marks an order as shipped.
type ShipOrderCmd struct {
	ID string
}

func (c ShipOrderCmd) AggregateID() string { return c.ID }

func (c ShipOrderCmd) EventName() string { return "OrderShipped" }

func (c ShipOrderCmd) ShouldSnapshot() bool { return true }

func (c ShipOrderCmd) Validate(
	current *domain.Order,
) error {
	if current == nil {
		return models.ErrValidation
	}

	if current.Status != domain.OrderConfirmed {
		return models.ErrValidation
	}

	return nil
}

func (c ShipOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	order := *current
	order.Status = domain.OrderShipped
	return order
}

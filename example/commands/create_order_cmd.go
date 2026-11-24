package commands

import (
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// CreateOrderCmd creates a new order.
type CreateOrderCmd struct {
	ID     string
	Total  float64
	Items  int
	ShipTo string
}

func (c CreateOrderCmd) AggregateID() string { return c.ID }

func (c CreateOrderCmd) EventName() string { return "OrderCreated" }

func (c CreateOrderCmd) ShouldSnapshot() bool { return false }

func (c CreateOrderCmd) Validate(
	current *domain.Order,
) error {
	if c.Total <= 0 {
		return models.ErrValidation
	}

	if c.Items <= 0 {
		return models.ErrValidation
	}

	if current != nil {
		return models.ErrValidation
	}

	return nil
}

func (c CreateOrderCmd) EmitEvent(
	current *domain.Order,
) domain.Order {
	return domain.Order{
		ID:     c.ID,
		Status: domain.OrderPending,
		Total:  c.Total,
		Items:  c.Items,
		ShipTo: c.ShipTo,
	}
}

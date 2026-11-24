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

// AggregateID returns the ID of the aggregate the command targets.
func (c CreateOrderCmd) AggregateID() string { return c.ID }

// EventName returns the name of the event the command emits.
func (c CreateOrderCmd) EventName() string { return "OrderCreated" }

// ShouldSnapshot reports whether a snapshot is written after the command.
func (c CreateOrderCmd) ShouldSnapshot() bool { return false }

// Validate checks the command against the current aggregate state.
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

// EmitEvent applies the command to the current aggregate and returns the resulting state.
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
